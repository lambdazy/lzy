package ai.lzy.fs;

import ai.lzy.fs.fs.*;
import ai.lzy.longrunning.IdempotencyUtils;
import ai.lzy.longrunning.LocalOperationService;
import ai.lzy.longrunning.LocalOperationService.ImmutableCopyOperation;
import ai.lzy.longrunning.Operation;
import ai.lzy.model.UriScheme;
import ai.lzy.model.grpc.ProtoConverter;
import ai.lzy.model.slot.Slot;
import ai.lzy.model.slot.SlotInstance;
import ai.lzy.util.grpc.ContextAwareTask;
import ai.lzy.util.grpc.JsonUtils;
import ai.lzy.util.grpc.ProtoPrinter;
import ai.lzy.v1.common.LMS;
import ai.lzy.v1.deprecated.LzyFsApi;
import ai.lzy.v1.deprecated.LzyFsGrpc;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import ai.lzy.v1.slots.LSA;
import ai.lzy.v1.slots.LzySlotsApiGrpc;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.StreamSupport;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static ai.lzy.longrunning.IdempotencyUtils.*;
import static ai.lzy.util.grpc.GrpcUtils.*;


public class SlotsService {
    private static final Logger LOG = LogManager.getLogger(SlotsService.class);

    private final String agentId;
    private final SlotsManager slotsManager;
    @Nullable
    private final LzyFSManager fsManager;
    private final ExecutorService longrunningExecutor;
    private final LocalOperationService operationService;
    private final LzySlotsApiGrpc.LzySlotsApiImplBase slotsApi;
    private final LzyFsGrpc.LzyFsImplBase legacyWrapper;

    public SlotsService(String agentId, LocalOperationService operationService,
                        SlotsManager slotsManager, @Nullable LzyFSManager fsManager)
    {
        this.agentId = agentId;
        this.slotsManager = slotsManager;
        this.fsManager = fsManager;
        this.operationService = operationService;

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
        this.legacyWrapper = new LegacyWrapper();
    }

    public void shutdown() {
        longrunningExecutor.shutdown();
    }

    public LzySlotsApiGrpc.LzySlotsApiImplBase getSlotsApi() {
        return slotsApi;
    }

    public LongRunningServiceGrpc.LongRunningServiceImplBase getLongrunningApi() {
        return operationService;
    }

    public LzyFsGrpc.LzyFsImplBase getLegacyWrapper() {
        return legacyWrapper;
    }

    private class SlotsApiImpl extends LzySlotsApiGrpc.LzySlotsApiImplBase {

        @Override
        public void createSlot(LSA.CreateSlotRequest request, StreamObserver<LSA.CreateSlotResponse> response) {
            Operation.IdempotencyKey idempotencyKey = IdempotencyUtils.getIdempotencyKey(request);

            if (idempotencyKey != null && loadExistingOpResult(operationService, idempotencyKey,
                LSA.CreateSlotResponse.class, response, "Cannot create slot", LOG))
            {
                return;
            }

            LOG.info("LzySlotsApi::createSlot: taskId={}, slotName={}: {}.",
                request.getTaskId(), request.getSlot().getName(), JsonUtils.printSingleLine(request));

            var op = Operation.create(agentId, "CreateSlot: " + request.getSlot().getName(), idempotencyKey, null);
            var opSnapshot = operationService.registerOperation(op);

            if (op.id().equals(opSnapshot.id())) {
                var existing = slotsManager.slot(request.getTaskId(), request.getSlot().getName());
                if (existing != null) {
                    var msg = "Slot `" + request.getSlot().getName() + "` already exists.";
                    var errorStatus = Status.ALREADY_EXISTS.withDescription(msg);
                    LOG.warn(msg);

                    operationService.updateError(op.id(), errorStatus);
                    response.onError(errorStatus.asRuntimeException());
                    return;
                }

                final Slot slotSpec = ProtoConverter.fromProto(request.getSlot());
                final LzySlot lzySlot =
                    slotsManager.getOrCreateSlot(request.getTaskId(), slotSpec, request.getChannelId());

                if (fsManager != null && lzySlot instanceof LzyFileSlot fileSlot) {
                    fsManager.addSlot(fileSlot);
                }

                operationService.updateResponse(op.id(), LSA.CreateSlotResponse.getDefaultInstance());
                response.onNext(LSA.CreateSlotResponse.getDefaultInstance());
                response.onCompleted();
                return;
            } else {
                LOG.info("Found operation by idempotency key: {}", opSnapshot.toString());
            }

            replyWaitedOpResult(operationService, opSnapshot.id(), response, LSA.CreateSlotResponse.class,
                "Cannot create slot", LOG);
        }

        @Override
        public void connectSlot(LSA.ConnectSlotRequest request, StreamObserver<LongRunning.Operation> response) {
            Operation.IdempotencyKey idempotencyKey = IdempotencyUtils.getIdempotencyKey(request);
            if (idempotencyKey != null && loadExistingOp(operationService, idempotencyKey, response, LOG)) {
                return;
            }

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
            if (!UriScheme.LzyFs.match(toSlot.uri())) {
                var msg = "Slot scheme not valid";
                LOG.error(msg + ": {}", toSlot.shortDesc());
                response.onError(Status.INVALID_ARGUMENT.withDescription(msg).asException());
                return;
            }

            if (slot instanceof LzyInputSlot inputSlot) {
                var op = Operation.create(agentId, "ConnectSlot: %s -> %s".formatted(
                    ProtoPrinter.printer().printToString(request.getFrom()),
                    ProtoPrinter.printer().printToString(request.getTo())
                ), null);
                ImmutableCopyOperation opSnapshot = operationService.registerOperation(op);

                response.onNext(opSnapshot.toProto());
                response.onCompleted();

                if (op.id().equals(opSnapshot.id())) {
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

                                operationService.updateResponse(op.id(), LSA.ConnectSlotResponse.getDefaultInstance());

                                LOG.info("[{}] ... connected", op.id());
                            } catch (Exception e) {
                                LOG.error("[{}] Cannot connect slots, {} -> {}: {}",
                                    op.id(), fromSlot.shortDesc(), toSlot.shortDesc(), e.getMessage(), e);
                                operationService.updateError(op.id(), Status.INTERNAL.withDescription(e.getMessage()));
                            }
                        }
                    });
                }
            } else {
                var msg = "Slot " + fromSlot.spec().name() + " not found in " + fromSlot.taskId();
                LOG.error(msg);
                response.onError(Status.NOT_FOUND.withDescription(msg).asException());
            }
        }

        @Override
        public void disconnectSlot(LSA.DisconnectSlotRequest request,
                                   StreamObserver<LSA.DisconnectSlotResponse> responseObserver)
        {
            Operation.IdempotencyKey idempotencyKey = IdempotencyUtils.getIdempotencyKey(request);

            if (idempotencyKey != null && loadExistingOpResult(operationService, idempotencyKey,
                LSA.DisconnectSlotResponse.class, responseObserver, "Cannot disconnect slot", LOG))
            {
                return;
            }

            LOG.info("LzySlotsApi::disconnectSlot `{}`: {}.",
                request.getSlotInstance().getSlotUri(), JsonUtils.printSingleLine(request));

            final SlotInstance slotInstance = ProtoConverter.fromProto(request.getSlotInstance());

            var op = Operation.create(agentId, "DisconnectSlot: " + slotInstance.name(), idempotencyKey, null);
            var opSnapshot = operationService.registerOperation(op);

            if (op.id().equals(opSnapshot.id())) {

                final LzySlot slot = slotsManager.slot(slotInstance.taskId(), slotInstance.name());
                if (slot == null) {
                    var msg = "Slot `" + slotInstance.shortDesc() + "` not found.";
                    var errorStatus = Status.NOT_FOUND.withDescription(msg);

                    LOG.error(msg);
                    operationService.updateError(op.id(), errorStatus);
                    responseObserver.onError(errorStatus.asRuntimeException());
                    return;
                }

                longrunningExecutor.submit(new ContextAwareTask() {
                    @Override
                    protected void execute() {
                        slot.suspend();
                    }
                });

                operationService.updateResponse(op.id(), LSA.DisconnectSlotResponse.getDefaultInstance());
                responseObserver.onNext(LSA.DisconnectSlotResponse.getDefaultInstance());
                responseObserver.onCompleted();
                return;
            } else {
                LOG.info("Found operation by idempotency key: {}", opSnapshot.toString());
            }

            replyWaitedOpResult(operationService, opSnapshot.id(), responseObserver, LSA.DisconnectSlotResponse.class,
                "Cannot disconnect slot", LOG);
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
            var idempotencyKey = IdempotencyUtils.getIdempotencyKey(request);

            if (idempotencyKey != null && loadExistingOpResult(operationService, idempotencyKey,
                LSA.DestroySlotResponse.class, response, "Cannot destroy slot", LOG))
            {
                return;
            }

            LOG.info("LzySlotsApi::destroySlot `{}`: {}.",
                request.getSlotInstance().getSlotUri(), JsonUtils.printSingleLine(request));

            final SlotInstance slotInstance = ProtoConverter.fromProto(request.getSlotInstance());

            var op = Operation.create(agentId, "DestroySlot: " + slotInstance.name(), idempotencyKey, null);
            var opSnapshot = operationService.registerOperation(op);

            if (op.id().equals(opSnapshot.id())) {
                final LzySlot slot = slotsManager.slot(slotInstance.taskId(), slotInstance.name());
                if (slot == null) {
                    var msg = "Slot `" + slotInstance.shortDesc() + "` not found.";
                    LOG.warn(msg);

                    operationService.updateResponse(op.id(), LSA.DestroySlotResponse.getDefaultInstance());
                    response.onNext(LSA.DestroySlotResponse.getDefaultInstance());
                    response.onCompleted();
                    // operationService.updateError(...);
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

                operationService.updateResponse(op.id(), LSA.DestroySlotResponse.getDefaultInstance());
                response.onNext(LSA.DestroySlotResponse.getDefaultInstance());
                response.onCompleted();

                return;
            } else {
                LOG.info("Found operation by idempotency key: {}", opSnapshot.toString());
            }

            replyWaitedOpResult(operationService, opSnapshot.id(), response, LSA.DestroySlotResponse.class,
                "Cannot destroy slot", LOG);
        }

        @Override
        public void openOutputSlot(LSA.SlotDataRequest request, StreamObserver<LSA.SlotDataChunk> response) {
            var idempotencyKey = IdempotencyUtils.getIdempotencyKey(request);

            if (idempotencyKey != null && loadExistingOpResult(operationService, idempotencyKey,
                /* for streaming data */ true, LSA.SlotDataChunk.class, response, "Cannot open output slot", LOG))
            {
                return;
            }

            LOG.info("LzySlotsApi::openOutputSlot {}: {}.",
                request.getSlotInstance().getSlotUri(), JsonUtils.printSingleLine(request));

            final SlotInstance slotInstance = ProtoConverter.fromProto(request.getSlotInstance());
            final String taskId = slotInstance.taskId();

            var op = Operation.create(agentId, "OpenOutputSlot with name: " + slotInstance.name(),
                idempotencyKey, null);
            var opSnapshot = operationService.registerOperation(op);

            if (op.id().equals(opSnapshot.id())) {
                final LzySlot slot = slotsManager.slot(taskId, slotInstance.name());
                if (slot instanceof LzyOutputSlot outputSlot) {
                    try {
                        var fullData = new ArrayList<LSA.SlotDataChunk>();
                        outputSlot
                            .readFromPosition(request.getOffset())
                            .forEach(chunk -> {
                                fullData.add(LSA.SlotDataChunk.newBuilder().setChunk(chunk).build());
                                response.onNext(LSA.SlotDataChunk.newBuilder().setChunk(chunk).build());
                            });

                        fullData.add(LSA.SlotDataChunk.newBuilder().setControl(LSA.SlotDataChunk.Control.EOS).build());
                        operationService.updateResponse(op.id(), fullData);
                        response.onNext(
                            LSA.SlotDataChunk.newBuilder().setControl(LSA.SlotDataChunk.Control.EOS).build());
                        response.onCompleted();
                    } catch (IOException e) {
                        var msg =
                            "IO error while reading slot %s: %s".formatted(slotInstance.shortDesc(), e.getMessage());
                        var errorStatus = Status.INTERNAL.withDescription(msg);

                        LOG.error(msg, e);
                        operationService.updateError(op.id(), errorStatus);
                        response.onError(errorStatus.asException());
                    }
                    return;
                }

                LOG.error("Cannot read from unknown slot " + slotInstance.uri());
                var errorStatus = Status.NOT_FOUND.withDescription("Reading from input slot: " + slotInstance.uri());
                operationService.updateError(op.id(), errorStatus);
                response.onError(errorStatus.asException());
                return;
            } else {
                LOG.info("Found operation by idempotency key: {}", opSnapshot.toString());
            }

            replyWaitedOpResult(operationService, opSnapshot.id(), response, LSA.SlotDataChunk.class,
                /* for streaming data */ true, "Cannot open output slot", LOG);
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

            if (errRef[0] != null) {
                LegacyWrapper.this.onError(errRef[0], resp);
                return;
            }

            if (!operationService.awaitOperationCompletion(opRef[0].getId(), Duration.ofSeconds(5))) {
                LOG.error("[{}] Cannot await operation completion: { opId: {} }", agentId, opRef[0].getId());
                resp.onError(Status.INTERNAL.withDescription("Cannot connect slot").asRuntimeException());
                return;
            }

            var op = operationService.get(opRef[0].getId());

            assert op != null;

            if (op.error() != null) {
                LegacyWrapper.this.onError(op.error().asRuntimeException(), resp);
            } else {
                resp.onNext(LzyFsApi.SlotCommandStatus.getDefaultInstance());
                resp.onCompleted();
            }
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
