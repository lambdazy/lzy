package ai.lzy.fs;

import ai.lzy.fs.fs.LzyFSManager;
import ai.lzy.fs.fs.LzyFileSlot;
import ai.lzy.fs.fs.LzyInputSlot;
import ai.lzy.fs.fs.LzyOutputSlot;
import ai.lzy.fs.fs.LzySlot;
import ai.lzy.longrunning.Operation;
import ai.lzy.model.UriScheme;
import ai.lzy.model.grpc.ProtoConverter;
import ai.lzy.model.slot.Slot;
import ai.lzy.model.slot.SlotInstance;
import ai.lzy.util.grpc.ContextAwareTask;
import ai.lzy.util.grpc.JsonUtils;
import ai.lzy.v1.common.LMS;
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
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.StreamSupport;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static ai.lzy.util.grpc.GrpcUtils.NO_AUTH_TOKEN;
import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;


public class SlotsService {
    private static final Logger LOG = LogManager.getLogger(SlotsService.class);

    private final String agentId;
    private final SlotsManager slotsManager;
    @Nullable
    private final LzyFSManager fsManager;
    private final ExecutorService longrunningExecutor;
    private final Map<String, Operation> operations = new ConcurrentHashMap<>();
    private final LzySlotsApiGrpc.LzySlotsApiImplBase slotsApi;
    private final LongRunningServiceGrpc.LongRunningServiceImplBase longrunningApi;
    private final LzyFsGrpc.LzyFsImplBase legacyWrapper;

    public SlotsService(String agentId, SlotsManager slotsManager, @Nullable LzyFSManager fsManager) {
        this.agentId = agentId;
        this.slotsManager = slotsManager;
        this.fsManager = fsManager;

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

    private class SlotsApiImpl extends LzySlotsApiGrpc.LzySlotsApiImplBase {
        @Override
        public void createSlot(LSA.CreateSlotRequest request, StreamObserver<LSA.CreateSlotResponse> response) {
            LOG.info("LzySlotsApi::createSlot: taskId={}, slotName={}: {}.",
                request.getTaskId(), request.getSlot().getName(), JsonUtils.printSingleLine(request));

            var existing = slotsManager.slot(request.getTaskId(), request.getSlot().getName());
            if (existing != null) {
                var msg = "Slot `" + request.getSlot().getName() + "` already exists.";
                LOG.warn(msg);
                response.onError(Status.ALREADY_EXISTS.withDescription(msg).asException());
                return;
            }

            final Slot slotSpec = ProtoConverter.fromProto(request.getSlot());
            final LzySlot lzySlot = slotsManager.getOrCreateSlot(request.getTaskId(), slotSpec, request.getChannelId());

            if (fsManager != null && lzySlot instanceof LzyFileSlot fileSlot) {
                fsManager.addSlot(fileSlot);
            }

            response.onNext(LSA.CreateSlotResponse.getDefaultInstance());
            response.onCompleted();
        }

        @Override
        public void connectSlot(LSA.ConnectSlotRequest request, StreamObserver<LongRunning.Operation> response) {
            LOG.info("LzySlotsApi::connectSlot from {} to {}: {}.",
                request.getFrom().getSlotUri(), request.getTo().getSlotUri(), JsonUtils.printSingleLine(request));

            final SlotInstance fromSlot = ProtoConverter.fromProto(request.getFrom());

            final LzySlot slot = slotsManager.slot(fromSlot.taskId(), fromSlot.name());
            if (slot == null) {
                var msg = "Slot `" + fromSlot.taskId() + "/" + fromSlot.name() + "` not found.";
                LOG.error(msg);
                response.onError(Status.NOT_FOUND.withDescription(msg).asException());
                return;
            }

            final SlotInstance toSlot = ProtoConverter.fromProto(request.getTo());
            if (slot instanceof LzyInputSlot inputSlot) {
                Validate.isTrue(UriScheme.LzyFs.match(toSlot.uri()));

                var op = new Operation(
                    agentId,
                    "ConnectSlot: %s -> %s".formatted(fromSlot.shortDesc(), toSlot.shortDesc()),
                    Any.pack(LSA.ConnectSlotMetadata.getDefaultInstance())
                );

                operations.put(op.id(), op);

                response.onNext(op.toProto());
                response.onCompleted();

                longrunningExecutor.submit(new ContextAwareTask() {
                    @Override
                    protected void execute() {
                        LOG.info("[{}] Trying to connect slots, {} -> {}...",
                            op.id(), fromSlot.shortDesc(), toSlot.shortDesc());
                        try {
                            var channel = newGrpcChannel(toSlot.uri().getHost(), toSlot.uri().getPort(),
                                LzySlotsApiGrpc.SERVICE_NAME);
                            var client = newBlockingClient(LzySlotsApiGrpc.newBlockingStub(channel), "LzyFs",
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

                            inputSlot.connect(toSlot.uri(), dataProvider);

                            synchronized (op) {
                                op.setResponse(Any.pack(LSA.ConnectSlotResponse.getDefaultInstance()));
                            }
                            LOG.info("[{}] ... connected", op.id());
                        } catch (Exception e) {
                            LOG.error("[{}] Cannot connect slots, {} -> {}: {}",
                                op.id(), fromSlot.shortDesc(), toSlot.shortDesc(), e.getMessage(), e);
                            synchronized (op) {
                                op.setError(Status.INTERNAL.withDescription(e.getMessage()));
                            }
                        }
                    }
                });
            } else {
                var msg = "Slot " + fromSlot.spec().name() + " not found in " + fromSlot.taskId();
                LOG.error(msg);
                response.onError(Status.NOT_FOUND.withDescription(msg).asException());
            }
        }

        @Override
        public void disconnectSlot(LSA.DisconnectSlotRequest request, StreamObserver<LSA.DisconnectSlotResponse> resp) {
            LOG.info("LzySlotsApi::disconnectSlot `{}`: {}.",
                request.getSlotInstance().getSlotUri(), JsonUtils.printSingleLine(request));

            final SlotInstance slotInstance = ProtoConverter.fromProto(request.getSlotInstance());

            final LzySlot slot = slotsManager.slot(slotInstance.taskId(), slotInstance.name());
            if (slot == null) {
                var msg = "Slot `" + slotInstance.shortDesc() + "` not found.";
                LOG.error(msg);
                resp.onError(Status.NOT_FOUND.withDescription(msg).asException());
                return;
            }

            longrunningExecutor.submit(new ContextAwareTask() {
                @Override
                protected void execute() {
                    slot.suspend();
                }
            });

            resp.onNext(LSA.DisconnectSlotResponse.getDefaultInstance());
            resp.onCompleted();
        }

        @Override
        public void statusSlot(LSA.StatusSlotRequest request, StreamObserver<LSA.StatusSlotResponse> response) {
            LOG.info("LzySlotsApi::statusSlot `{}`: {}.",
                request.getSlotInstance().getSlotUri(), JsonUtils.printSingleLine(request));

            final SlotInstance slotInstance = ProtoConverter.fromProto(request.getSlotInstance());

            final LzySlot slot = slotsManager.slot(slotInstance.taskId(), slotInstance.name());
            if (slot == null) {
                var msg = "Slot `" + slotInstance.shortDesc() + "` not found.";
                LOG.error(msg);
                response.onError(Status.NOT_FOUND.withDescription(msg).asException());
                return;
            }

            response.onNext(
                LSA.StatusSlotResponse.newBuilder()
                    .setStatus(
                        LMS.SlotStatus.newBuilder(slot.status())
                            .setTaskId(slotInstance.taskId())
                            .build())
                    .build());
            response.onCompleted();
        }

        @Override
        public void destroySlot(LSA.DestroySlotRequest request, StreamObserver<LSA.DestroySlotResponse> response) {
            LOG.info("LzySlotsApi::destroySlot `{}`: {}.",
                request.getSlotInstance().getSlotUri(), JsonUtils.printSingleLine(request));

            final SlotInstance slotInstance = ProtoConverter.fromProto(request.getSlotInstance());

            final LzySlot slot = slotsManager.slot(slotInstance.taskId(), slotInstance.name());
            if (slot == null) {
                var msg = "Slot `" + slotInstance.shortDesc() + "` not found.";
                LOG.warn(msg);
                response.onNext(LSA.DestroySlotResponse.getDefaultInstance());
                response.onCompleted();
                // response.onError(Status.NOT_FOUND.withDescription(msg).asException());
                return;
            }

            longrunningExecutor.submit(new ContextAwareTask() {
                @Override
                protected void execute() {
                    LOG.info("Explicitly closing slot {}", slotInstance.shortDesc());
                    slot.destroy();
                    if (fsManager != null) {
                        fsManager.removeSlot(slot.name());
                    }
                }
            });

            response.onNext(LSA.DestroySlotResponse.getDefaultInstance());
            response.onCompleted();
        }

        @Override
        public void openOutputSlot(LSA.SlotDataRequest request, StreamObserver<LSA.SlotDataChunk> response) {
            LOG.info("LzySlotsApi::openOutputSlot {}: {}.",
                request.getSlotInstance().getSlotUri(), JsonUtils.printSingleLine(request));

            final SlotInstance slotInstance = ProtoConverter.fromProto(request.getSlotInstance());
            final String taskId = slotInstance.taskId();

            final LzySlot slot = slotsManager.slot(taskId, slotInstance.name());
            if (slot instanceof LzyOutputSlot outputSlot) {
                try {
                    outputSlot
                        .readFromPosition(request.getOffset())
                        .forEach(chunk -> response.onNext(LSA.SlotDataChunk.newBuilder().setChunk(chunk).build()));

                    response.onNext(
                        LSA.SlotDataChunk.newBuilder()
                            .setControl(LSA.SlotDataChunk.Control.EOS)
                            .build());
                    response.onCompleted();
                } catch (IOException e) {
                    var msg = "IO error while reading slot %s: %s".formatted(slotInstance.shortDesc(), e.getMessage());
                    LOG.error(msg, e);
                    response.onError(Status.INTERNAL.withDescription(msg).asException());
                }
                return;
            }

            LOG.error("Cannot read from unknown slot " + slotInstance.uri());
            response.onError(Status.NOT_FOUND
                .withDescription("Reading from input slot: " + slotInstance.uri())
                .asException());
        }
    }

    private class LongRunningApiImpl extends LongRunningServiceGrpc.LongRunningServiceImplBase {
        @Override
        public void get(LongRunning.GetOperationRequest request, StreamObserver<LongRunning.Operation> response) {
            LOG.info("LzySlotsLRApi::get op {}.", request.getOperationId());

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
            slotsApi.createSlot(
                LSA.CreateSlotRequest.newBuilder()
                    .setTaskId(request.getTaskId())
                    .setSlot(request.getSlot())
                    .setChannelId(request.getChannelId())
                    .build(),
                new StreamObserver<>() {
                    @Override
                    public void onNext(LSA.CreateSlotResponse value) {
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
