package ai.lzy.portal;

import ai.lzy.fs.fs.LzyInputSlot;
import ai.lzy.fs.fs.LzyOutputSlot;
import ai.lzy.fs.fs.LzySlot;
import ai.lzy.longrunning.Operation;
import ai.lzy.model.grpc.ProtoConverter;
import ai.lzy.model.slot.SlotInstance;
import ai.lzy.portal.slots.SnapshotProvider;
import ai.lzy.portal.slots.StdoutSlot;
import ai.lzy.util.grpc.ContextAwareTask;
import ai.lzy.v1.deprecated.LzyFsApi;
import ai.lzy.v1.deprecated.LzyFsGrpc;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import ai.lzy.v1.slots.LSA;
import ai.lzy.v1.slots.LzySlotsApiGrpc;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.stream.StreamSupport;
import javax.annotation.Nonnull;

import static ai.lzy.util.grpc.GrpcUtils.NO_AUTH_TOKEN;
import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;

public class PortalSlotsService {
    private static final Logger LOG = LogManager.getLogger(PortalSlotsService.class);

    private final Portal portal;
    private final ExecutorService longrunningExecutor;
    private final Map<String, Operation> operations = new ConcurrentHashMap<>();
    private final LzySlotsApiGrpc.LzySlotsApiImplBase slotsApi;
    private final LongRunningServiceGrpc.LongRunningServiceImplBase longrunningApi;
    private final LzyFsGrpc.LzyFsImplBase legacyWrapper;

    public PortalSlotsService(Portal portal) {
        this.portal = portal;

        this.longrunningExecutor = new ThreadPoolExecutor(5, 20, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<>(),
            new ThreadFactory() {
                private final AtomicInteger counter = new AtomicInteger(1);

                @Override
                public Thread newThread(@Nonnull Runnable r) {
                    var th = new Thread(r, "lr-slots-" + counter.getAndIncrement());
                    th.setUncaughtExceptionHandler(
                        (t, e) -> LOG.error("Unexpected exception in thread {}: {}", t.getName(), e.getMessage(), e));
                    return th;
                }
            });

        this.slotsApi = new SlotsApiImpl();
        this.longrunningApi = new LongRunningApiImpl();
        this.legacyWrapper = new LegacyWrapper();
    }

    public void shutdown() {
        longrunningExecutor.shutdown();
    }

    public LzySlotsApiGrpc.LzySlotsApiImplBase getSlotsApi() {
        return slotsApi;
    }

    public LongRunningServiceGrpc.LongRunningServiceImplBase getLongrunningApi() {
        return longrunningApi;
    }

    public LzyFsGrpc.LzyFsImplBase getLegacyWrapper() {
        return legacyWrapper;
    }

    private class SlotsApiImpl  extends LzySlotsApiGrpc.LzySlotsApiImplBase {
        @Override
        public void createSlot(LSA.CreateSlotRequest request, StreamObserver<LSA.CreateSlotResponse> response) {
            response.onError(Status.UNIMPLEMENTED.withDescription("Not supported in portal").asException());
        }

        @Override
        public synchronized void connectSlot(LSA.ConnectSlotRequest request,
                                             StreamObserver<LongRunning.Operation> response)
        {
            final SlotInstance from = ProtoConverter.fromProto(request.getFrom());
            final SlotInstance to = ProtoConverter.fromProto(request.getTo());
            LOG.info("Connect portal slot, taskId: {}, slotName: {}, remoteSlotUri: {}",
                from.taskId(), from.name(), to.uri());

            Consumer<LzyInputSlot> startConnect = inputSlot -> {
                try {
                    var op = new Operation(
                        portal.getPortalId(),
                        "ConnectSlot: %s -> %s".formatted(from.shortDesc(), to.shortDesc()),
                        Any.pack(LSA.ConnectSlotMetadata.getDefaultInstance())
                    );

                    operations.put(op.id(), op);

                    response.onNext(op.toProto());
                    response.onCompleted();

                    // TODO: MDC & GrpcConntext
                    longrunningExecutor.submit(new ContextAwareTask() {
                        @Override
                        protected void execute() {
                            LOG.info("[{}] Trying to connect slots, {} -> {}...",
                                op.id(), from.shortDesc(), to.shortDesc());

                            try {
                                var channel = newGrpcChannel(to.uri().getHost(), to.uri().getPort(),
                                    LzySlotsApiGrpc.SERVICE_NAME);
                                var client = newBlockingClient(LzySlotsApiGrpc.newBlockingStub(channel), "PortalSlots",
                                    NO_AUTH_TOKEN);

                                var req = LSA.SlotDataRequest.newBuilder()
                                    .setSlotInstance(request.getTo())
                                    .setOffset(0)
                                    .build();

                                var msgIter = client.openOutputSlot(req);

                                var dataProvider = StreamSupport
                                    .stream(Spliterators.spliteratorUnknownSize(msgIter, Spliterator.NONNULL), false)
                                    .map(msg -> msg.hasChunk() ? msg.getChunk() : ByteString.EMPTY)
                                    .onClose(channel::shutdownNow);

                                inputSlot.connect(to.uri(), dataProvider);

                                synchronized (op) {
                                    op.setResponse(Any.pack(LSA.ConnectSlotResponse.getDefaultInstance()));
                                }
                                LOG.info("[{}] ... connected", op.id());
                            } catch (Exception e) {
                                LOG.error("[{}] Cannot connect slots, {} -> {}: {}",
                                    op.id(), from.shortDesc(), to.shortDesc(), e.getMessage(), e);
                                synchronized (op) {
                                    op.setError(Status.INTERNAL.withDescription(e.getMessage()));
                                }
                            }
                        }
                    });
                } catch (StatusRuntimeException e) {
                    LOG.error("Failed to connect to remote slot: {}", e.getMessage(), e);
                    response.onError(Status.ABORTED.withCause(e).asException());
                }
            };

            LzyInputSlot lzyInputSlot = portal.getSnapshots().getInputSlot(from.name());
            if (lzyInputSlot != null) {
                if (lzyInputSlot.name().equals(from.name())) {
                    startConnect.accept(lzyInputSlot);
                    return;
                }

                LOG.error("Got connect to unexpected slot '{}', expected input slot '{}'",
                    from.name(), lzyInputSlot.name());
                response.onError(Status.INVALID_ARGUMENT.withDescription("Unexpected slot").asException());
                return;
            }

            var stdoutPeerSlot = portal.findOutSlot(from.name());
            if (stdoutPeerSlot != null) {
                startConnect.accept(stdoutPeerSlot);
                return;
            }

            var stderrPeerSlot = portal.findErrSlot(from.name());
            if (stderrPeerSlot != null) {
                startConnect.accept(stderrPeerSlot);
                return;
            }

            LOG.error("Only snapshot is supported now, got connect from `{}` to `{}`", from, to);
            response.onError(Status.INVALID_ARGUMENT.withDescription("Only snapshot is supported now").asException());
        }

        @Override
        public synchronized void disconnectSlot(LSA.DisconnectSlotRequest request,
                                                StreamObserver<LSA.DisconnectSlotResponse> response)
        {
            final SlotInstance slotInstance = ProtoConverter.fromProto(request.getSlotInstance());
            LOG.info("Disconnect portal slot, taskId: {}, slotName: {}", slotInstance.taskId(), slotInstance.name());

            assert slotInstance.taskId().isEmpty() : slotInstance.taskId();
            var slotName = slotInstance.name();

            boolean done = false;

            SnapshotProvider snapshots = portal.getSnapshots();
            LzyInputSlot inputSlot = snapshots.getInputSlot(slotName);
            LzyOutputSlot outputSlot = snapshots.getOutputSlot(slotName);
            if (inputSlot != null) {
                inputSlot.disconnect();
                done = true;
            }
            if (outputSlot != null) {
                outputSlot.suspend();
                done = true;
            }

            if (!done) {
                inputSlot = portal.findOutSlot(slotName);
                if (inputSlot != null) {
                    inputSlot.disconnect();
                    done = true;
                }
            }

            if (!done) {
                inputSlot = portal.findErrSlot(slotName);
                if (inputSlot != null) {
                    inputSlot.disconnect();
                    done = true;
                }
            }

            StdoutSlot out = portal.getStdoutSlot();
            if (!done && out.name().equals(slotName)) {
                out.suspend();
                done = true;
            }

            StdoutSlot err = portal.getStderrSlot();
            if (!done && err.name().equals(slotName)) {
                err.suspend();
                done = true;
            }

            if (done) {
                response.onNext(LSA.DisconnectSlotResponse.getDefaultInstance());
                response.onCompleted();
                return;
            }

            LOG.error("Only snapshot or stdout/stderr are supported now, got {}", slotInstance);
            response.onError(Status.INVALID_ARGUMENT
                .withDescription("Only snapshot or stdout/stderr are supported now").asException());
        }

        @Override
        public synchronized void statusSlot(LSA.StatusSlotRequest request,
                                            StreamObserver<LSA.StatusSlotResponse> response)
        {
            final SlotInstance slotInstance = ProtoConverter.fromProto(request.getSlotInstance());
            LOG.info("Status portal slot, taskId: {}, slotName: {}", slotInstance.taskId(), slotInstance.name());

            if (!portal.getPortalId().equals(slotInstance.taskId())) {
                LOG.error("Unknown task " + slotInstance.taskId());
                response.onError(Status.INVALID_ARGUMENT
                    .withDescription("Unknown task " + slotInstance.taskId()).asException());
                return;
            }

            Consumer<LzySlot> reply = slot -> {
                response.onNext(
                    LSA.StatusSlotResponse.newBuilder()
                        .setStatus(slot.status())
                        .build());
                response.onCompleted();
            };

            SnapshotProvider snapshots = portal.getSnapshots();
            for (var slot : snapshots.getInputSlots()) {
                if (slot.name().equals(slotInstance.name())) {
                    reply.accept(slot);
                    return;
                }
            }

            for (var slot : snapshots.getOutputSlots()) {
                reply.accept(slot);
                return;
            }

            for (var stdSlot : portal.getOutErrSlots()) {
                if (stdSlot.name().equals(slotInstance.name())) {
                    reply.accept(stdSlot);
                    return;
                }

                var slot = stdSlot.find(slotInstance.name());
                if (slot != null) {
                    reply.accept(slot);
                    return;
                }
            }

            LOG.error("Slot '" + slotInstance.name() + "' not found");
            response.onError(Status.NOT_FOUND
                .withDescription("Slot '" + slotInstance.name() + "' not found").asException());
        }

        @Override
        public synchronized void destroySlot(LSA.DestroySlotRequest request,
                                             StreamObserver<LSA.DestroySlotResponse> response)
        {
            final SlotInstance slotInstance = ProtoConverter.fromProto(request.getSlotInstance());
            LOG.info("Destroy portal slot, taskId: {}, slotName: {}", slotInstance.taskId(), slotInstance.name());
            var slotName = slotInstance.name();

            boolean done = false;

            SnapshotProvider snapshots = portal.getSnapshots();
            LzyInputSlot inputSlot = snapshots.getInputSlot(slotName);
            LzyOutputSlot outputSlot = snapshots.getOutputSlot(slotName);
            if (inputSlot != null) {
                inputSlot.destroy();
                snapshots.removeInputSlot(slotName);
                done = true;
            }
            if (outputSlot != null) {
                outputSlot.destroy();
                snapshots.removeOutputSlot(slotName);
                done = true;
            }

            if (!done) {
                inputSlot = portal.findOutSlot(slotName);
                if (inputSlot != null) {
                    inputSlot.destroy();
                    done = true;
                }
            }

            if (!done) {
                inputSlot = portal.findErrSlot(slotName);
                if (inputSlot != null) {
                    inputSlot.destroy();
                    done = true;
                }
            }

            StdoutSlot out = portal.getStdoutSlot();
            if (!done && out.name().equals(slotName)) {
                out.destroy();
                done = true;
            }

            StdoutSlot err = portal.getStderrSlot();
            if (!done && err.name().equals(slotName)) {
                err.destroy();
                done = true;
            }

            if (done) {
                response.onNext(LSA.DestroySlotResponse.getDefaultInstance());
                response.onCompleted();
                return;
            }

            LOG.error("Only snapshot or stdout/stderr are supported now, got {}", slotInstance);
            response.onError(Status.INVALID_ARGUMENT
                .withDescription("Only snapshot or stdout/stderr are supported now").asException());
        }

        @Override
        public void openOutputSlot(LSA.SlotDataRequest request, StreamObserver<LSA.SlotDataChunk> response) {
            final SlotInstance slotInstance = ProtoConverter.fromProto(request.getSlotInstance());
            LOG.info("Open portal output slot, uri: {}, offset: {}", slotInstance.uri(), request.getOffset());
            final var slotUri = slotInstance.uri();
            final var slotName = slotUri.getPath().substring(portal.getPortalId().length() + 1);

            Consumer<LzyOutputSlot> reader = outputSlot -> {
                try {
                    outputSlot
                        .readFromPosition(request.getOffset())
                        .forEach(chunk -> response.onNext(LSA.SlotDataChunk.newBuilder().setChunk(chunk).build()));

                    response.onNext(LSA.SlotDataChunk.newBuilder().setControl(LSA.SlotDataChunk.Control.EOS).build());
                    response.onCompleted();
                } catch (Exception e) {
                    LOG.error("Error while uploading data: {}", e.getMessage(), e);
                    response.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
                }
            };

            LzyOutputSlot outputSlot;

            synchronized (this) {
                outputSlot = portal.getSnapshots().getOutputSlot(slotName);
                if (outputSlot == null) {
                    StdoutSlot out = portal.getStdoutSlot();
                    StdoutSlot err = portal.getStderrSlot();
                    if (out.name().equals(slotName)) {
                        outputSlot = out;
                    } else if (err.name().equals(slotName)) {
                        outputSlot = err;
                    }
                }
            }

            if (outputSlot != null) {
                reader.accept(outputSlot);
                return;
            }

            LOG.error("Only snapshot or stdout/stderr are supported now, got {}", slotInstance);
            response.onError(Status.INVALID_ARGUMENT
                .withDescription("Only snapshot or stdout/stderr are supported now").asException());
        }
    }

    private class LongRunningApiImpl extends LongRunningServiceGrpc.LongRunningServiceImplBase {
        @Override
        public void get(LongRunning.GetOperationRequest request, StreamObserver<LongRunning.Operation> response) {
            LOG.info("PortalSlotsLRApi::get op {}.", request.getOperationId());

            var op = operations.get(request.getOperationId());
            if (op == null) {
                var msg = "Operation %s not found".formatted(request.getOperationId());
                LOG.error(msg);
                response.onError(Status.NOT_FOUND.withDescription(msg).asException());
                return;
            }

            synchronized (op) {
                if (op.done()) {
                    if (op.response() != null) {
                        LOG.info("Operation {} successfully completed.", op.id());
                    } else if (op.error() != null) {
                        LOG.info("Operation {} failed with error {}.", op.id(), op.error());
                    } else {
                        LOG.error("Operation {} is in unknown completed state {}.", op.id(), op.toString());
                    }
                    operations.remove(op.id());
                }
            }

            response.onNext(op.toProto());
            response.onCompleted();
        }
    }

    private class LegacyWrapper extends LzyFsGrpc.LzyFsImplBase {
        @Override
        public void createSlot(LzyFsApi.CreateSlotRequest request, StreamObserver<LzyFsApi.SlotCommandStatus> resp) {
            resp.onError(Status.UNIMPLEMENTED.withDescription("Not supported in portal").asException());
        }

        @Override
        public void connectSlot(LzyFsApi.ConnectSlotRequest request, StreamObserver<LzyFsApi.SlotCommandStatus> resp) {
            final LongRunning.Operation[] opRef = {null};
            final Throwable[] errRef = {null};

            slotsApi.connectSlot(
                LSA.ConnectSlotRequest.newBuilder()
                    .setFrom(request.getFrom())
                    .setTo(request.getTo())
                    .build(),
                new StreamObserver<>() {
                    @Override
                    public void onNext(LongRunning.Operation value) {
                        opRef[0] = value;
                    }

                    @Override
                    public void onError(Throwable t) {
                        errRef[0] = t;
                    }

                    @Override
                    public void onCompleted() {
                        System.out.println("1");
                    }
                }
            );

            if (opRef[0] != null) {
                while (true) {
                    LockSupport.parkNanos(Duration.ofMillis(50).toNanos());
                    var op = operations.get(opRef[0].getId());
                    if (op != null) {
                        synchronized (op) {
                            if (op.done()) {
                                operations.remove(op.id());
                                resp.onNext(LzyFsApi.SlotCommandStatus.getDefaultInstance());
                                resp.onCompleted();
                                return;
                            }
                        }
                    } else {
                        resp.onError(Status.INTERNAL.withDescription("Smth goes wrong").asException());
                        return;
                    }
                }
            }

            Objects.requireNonNull(errRef[0]);
            LegacyWrapper.this.onError(errRef[0], resp);
        }

        @Override
        public void disconnectSlot(LzyFsApi.DisconnectSlotRequest request,
                                   StreamObserver<LzyFsApi.SlotCommandStatus> response)
        {
            slotsApi.disconnectSlot(
                LSA.DisconnectSlotRequest.newBuilder()
                    .setSlotInstance(request.getSlotInstance())
                    .build(),
                new StreamObserver<>() {
                    @Override
                    public void onNext(LSA.DisconnectSlotResponse value) {
                        response.onNext(LzyFsApi.SlotCommandStatus.getDefaultInstance());
                    }

                    @Override
                    public void onError(Throwable t) {
                        LegacyWrapper.this.onError(t, response);
                    }

                    @Override
                    public void onCompleted() {
                        response.onCompleted();
                    }
                }
            );
        }

        @Override
        public void statusSlot(LzyFsApi.StatusSlotRequest request, StreamObserver<LzyFsApi.SlotCommandStatus> resp) {
            slotsApi.statusSlot(
                LSA.StatusSlotRequest.newBuilder()
                    .setSlotInstance(request.getSlotInstance())
                    .build(),
                new StreamObserver<>() {
                    @Override
                    public void onNext(LSA.StatusSlotResponse value) {
                        resp.onNext(LzyFsApi.SlotCommandStatus.newBuilder()
                            .setStatus(value.getStatus())
                            .build());
                    }

                    @Override
                    public void onError(Throwable t) {
                        LegacyWrapper.this.onError(t, resp);
                    }

                    @Override
                    public void onCompleted() {
                        resp.onCompleted();
                    }
                }
            );
        }

        @Override
        public void destroySlot(LzyFsApi.DestroySlotRequest request, StreamObserver<LzyFsApi.SlotCommandStatus> resp) {
            slotsApi.destroySlot(
                LSA.DestroySlotRequest.newBuilder()
                    .setSlotInstance(request.getSlotInstance())
                    .build(),
                new StreamObserver<>() {
                    @Override
                    public void onNext(LSA.DestroySlotResponse value) {
                        resp.onNext(LzyFsApi.SlotCommandStatus.getDefaultInstance());
                    }

                    @Override
                    public void onError(Throwable t) {
                        LegacyWrapper.this.onError(t, resp);
                    }

                    @Override
                    public void onCompleted() {
                        resp.onCompleted();
                    }
                }
            );
        }

        @Override
        public void openOutputSlot(LzyFsApi.SlotRequest request, StreamObserver<LzyFsApi.Message> response) {
            response.onError(Status.UNIMPLEMENTED.withDescription("Legacy API").asException());
        }

        private void onError(Throwable t, StreamObserver<LzyFsApi.SlotCommandStatus> response) {
            if (t instanceof StatusRuntimeException e) {
                if (e.getStatus().getCode() == Status.Code.NOT_FOUND ||
                    e.getStatus().getCode() == Status.Code.ALREADY_EXISTS)
                {
                    response.onNext(LzyFsApi.SlotCommandStatus.newBuilder()
                        .setRc(LzyFsApi.SlotCommandStatus.RC.newBuilder()
                            .setCode(LzyFsApi.SlotCommandStatus.RC.Code.ERROR)
                            .setDescription(e.getStatus().getDescription())
                            .build())
                        .build());
                    response.onCompleted();
                    return;
                }
            }
            response.onError(t);
        }
    }
}
