package ai.lzy.fs;

import ai.lzy.fs.fs.LzyFSManager;
import ai.lzy.fs.fs.LzyFileSlot;
import ai.lzy.fs.fs.LzyInputSlot;
import ai.lzy.fs.fs.LzyOutputSlot;
import ai.lzy.fs.fs.LzySlot;
import ai.lzy.longrunning.IdempotencyUtils;
import ai.lzy.longrunning.LocalOperationService;
import ai.lzy.longrunning.LocalOperationService.OperationSnapshot;
import ai.lzy.longrunning.LocalOperationServiceUtils;
import ai.lzy.longrunning.Operation;
import ai.lzy.model.UriScheme;
import ai.lzy.model.grpc.ProtoConverter;
import ai.lzy.model.slot.Slot;
import ai.lzy.model.slot.SlotInstance;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.util.grpc.ContextAwareTask;
import ai.lzy.util.grpc.JsonUtils;
import ai.lzy.util.grpc.ProtoPrinter;
import ai.lzy.v1.common.LMS;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import ai.lzy.v1.slots.LSA;
import ai.lzy.v1.slots.LSA.DisconnectSlotRequest;
import ai.lzy.v1.slots.LSA.DisconnectSlotResponse;
import ai.lzy.v1.slots.LzySlotsApiGrpc;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.StreamSupport;

import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;


public class SlotsService {
    private static final Logger LOG = LogManager.getLogger(SlotsService.class);

    private final String agentId;
    private final SlotsManager slotsManager;
    @Nullable
    private final LzyFSManager fsManager;
    private final ExecutorService longrunningExecutor;
    private final LocalOperationService operationService;
    private final LzySlotsApiGrpc.LzySlotsApiImplBase slotsApi;
    private final RenewableJwt token;

    public SlotsService(String agentId, LocalOperationService operationService,
                        SlotsManager slotsManager, @Nullable LzyFSManager fsManager, RenewableJwt token)
    {
        this.agentId = agentId;
        this.slotsManager = slotsManager;
        this.fsManager = fsManager;
        this.operationService = operationService;
        this.token = token;

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

    private class SlotsApiImpl extends LzySlotsApiGrpc.LzySlotsApiImplBase {

        @Override
        public void createSlot(LSA.CreateSlotRequest request, StreamObserver<LSA.CreateSlotResponse> response) {
            Operation.IdempotencyKey idempotencyKey = IdempotencyUtils.getIdempotencyKey(request);

            if (idempotencyKey != null &&
                loadExistingOpResult(idempotencyKey, LSA.CreateSlotResponse.class, response, "Cannot create slot"))
            {
                return;
            }

            LOG.info("LzySlotsApi::createSlot: taskId={}, slotName={}: {}.",
                request.getTaskId(), request.getSlot().getName(), JsonUtils.printSingleLine(request));

            var op = Operation.create(agentId, "CreateSlot: " + request.getSlot().getName(), (Duration) null,
                idempotencyKey, null);
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

                Slot slotSpec = ProtoConverter.fromProto(request.getSlot());
                LzySlot lzySlot = slotsManager.getOrCreateSlot(request.getTaskId(), slotSpec, request.getChannelId());

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

            awaitOpAndReply(opSnapshot.id(), LSA.CreateSlotResponse.class, response, "Cannot create slot");
        }

        @Override
        public void connectSlot(LSA.ConnectSlotRequest request, StreamObserver<LongRunning.Operation> response) {
            Operation.IdempotencyKey idempotencyKey = IdempotencyUtils.getIdempotencyKey(request);
            if (idempotencyKey != null && loadExistingOp(idempotencyKey, response)) {
                return;
            }

            LOG.info("LzySlotsApi::connectSlot from {} to {}: {}.",
                request.getFrom().getSlotUri(), request.getTo().getSlotUri(), JsonUtils.printSingleLine(request));

            final SlotInstance fromSlot = ProtoConverter.fromProto(request.getFrom());
            final LzySlot slot = slotsManager.slot(fromSlot.taskId(), fromSlot.name());
            if (slot == null) {
                var msg = "Slot `" + fromSlot.taskId() + "/" + fromSlot.name() + "` not found.";
                LOG.error(msg);
                response.onError(Status.NOT_FOUND.withDescription(msg).asRuntimeException());
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
                var op = Operation.create(
                    agentId,
                    "ConnectSlot: %s -> %s".formatted(
                        ProtoPrinter.printer().printToString(request.getFrom()),
                        ProtoPrinter.printer().printToString(request.getTo())),
                    /* deadline */ null,
                    /* meta */ null);
                OperationSnapshot opSnapshot = operationService.registerOperation(op);

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
                                    () -> token.get().token());

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
        public void disconnectSlot(DisconnectSlotRequest request, StreamObserver<DisconnectSlotResponse> response) {
            Operation.IdempotencyKey idempotencyKey = IdempotencyUtils.getIdempotencyKey(request);

            if (idempotencyKey != null &&
                loadExistingOpResult(idempotencyKey, DisconnectSlotResponse.class, response, "Cannot disconnect slot"))
            {
                return;
            }

            LOG.info("LzySlotsApi::disconnectSlot `{}`: {}.",
                request.getSlotInstance().getSlotUri(), JsonUtils.printSingleLine(request));

            final SlotInstance slotInstance = ProtoConverter.fromProto(request.getSlotInstance());

            var op = Operation.create(agentId, "DisconnectSlot: " + slotInstance.name(), null, idempotencyKey, null);
            var opSnapshot = operationService.registerOperation(op);

            if (op.id().equals(opSnapshot.id())) {
                final LzySlot slot = slotsManager.slot(slotInstance.taskId(), slotInstance.name());
                if (slot == null) {
                    var msg = "Slot `" + slotInstance.shortDesc() + "` not found.";
                    var errorStatus = Status.NOT_FOUND.withDescription(msg);
                    LOG.error(msg);
                    operationService.updateError(op.id(), errorStatus);
                    response.onError(errorStatus.asRuntimeException());
                    return;
                }

                longrunningExecutor.submit(new ContextAwareTask() {
                    @Override
                    protected void execute() {
                        slot.suspend();
                    }
                });

                operationService.updateResponse(op.id(), DisconnectSlotResponse.getDefaultInstance());
                response.onNext(DisconnectSlotResponse.getDefaultInstance());
                response.onCompleted();
            } else {
                LOG.info("Found operation by idempotency key: {}", opSnapshot.toString());

                awaitOpAndReply(opSnapshot.id(), DisconnectSlotResponse.class, response, "Cannot disconnect slot");
            }
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

            if (idempotencyKey != null &&
                loadExistingOpResult(idempotencyKey, LSA.DestroySlotResponse.class, response, "Cannot destroy slot"))
            {
                return;
            }

            LOG.info("LzySlotsApi::destroySlot `{}`: {}.",
                request.getSlotInstance().getSlotUri(), JsonUtils.printSingleLine(request));

            final SlotInstance slotInstance = ProtoConverter.fromProto(request.getSlotInstance());

            var op = Operation.create(agentId, "DestroySlot: " + slotInstance.name(), null, idempotencyKey, null);
            var opSnapshot = operationService.registerOperation(op);

            if (op.id().equals(opSnapshot.id())) {
                final LzySlot slot = slotsManager.slot(slotInstance.taskId(), slotInstance.name());
                if (slot == null) {
                    var msg = "Slot `" + slotInstance.shortDesc() + "` not found.";
                    var errorStatus = Status.NOT_FOUND.withDescription(msg);
                    LOG.warn(msg);
                    operationService.updateError(op.id(), errorStatus);
                    response.onError(errorStatus.asRuntimeException());
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
            } else {
                LOG.info("Found operation by idempotency key: {}", opSnapshot.toString());

                awaitOpAndReply(opSnapshot.id(), LSA.DestroySlotResponse.class, response, "Cannot destroy slot");
            }
        }

        @Override
        public void openOutputSlot(LSA.SlotDataRequest request, StreamObserver<LSA.SlotDataChunk> response) {
            LOG.info("LzySlotsApi::openOutputSlot {}: {}.",
                request.getSlotInstance().getSlotUri(), JsonUtils.printSingleLine(request));

            final SlotInstance slotInstance = ProtoConverter.fromProto(request.getSlotInstance());
            final String taskId = slotInstance.taskId();

            final LzySlot slot = slotsManager.slot(taskId, slotInstance.name());
            if (slot instanceof LzyOutputSlot outputSlot) {

                outputSlot.readFromPosition(request.getOffset(), response);
                return;
            }

            LOG.error("Cannot read from unknown slot " + slotInstance.uri());
            response.onError(Status.NOT_FOUND
                .withDescription("Reading from input slot: " + slotInstance.uri())
                .asException());
        }

        private <T extends Message> boolean loadExistingOpResult(Operation.IdempotencyKey key, Class<T> respType,
                                                                 StreamObserver<T> response, String errorMsg)
        {
            return IdempotencyUtils.loadExistingOpResult(operationService, key, respType, response, errorMsg, LOG);
        }

        private boolean loadExistingOp(Operation.IdempotencyKey key, StreamObserver<LongRunning.Operation> response) {
            return IdempotencyUtils.loadExistingOp(operationService, key, response, LOG);
        }

        private <T extends Message> void awaitOpAndReply(String opId, Class<T> respType,
                                                         StreamObserver<T> response, String errorMsg)
        {
            LocalOperationServiceUtils.awaitOpAndReply(operationService, opId, response, respType, errorMsg, LOG);
        }
    }

}
