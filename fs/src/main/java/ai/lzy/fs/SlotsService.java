package ai.lzy.fs;

import ai.lzy.fs.fs.LzyFSManager;
import ai.lzy.fs.fs.LzyFileSlot;
import ai.lzy.fs.fs.LzyInputSlot;
import ai.lzy.fs.fs.LzyOutputSlot;
import ai.lzy.fs.fs.LzySlot;
import ai.lzy.longrunning.Operation;
import ai.lzy.model.grpc.ProtoConverter;
import ai.lzy.model.slot.Slot;
import ai.lzy.model.slot.SlotInstance;
import ai.lzy.util.grpc.JsonUtils;
import ai.lzy.v1.common.LMS;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import ai.lzy.v1.slots.LSA;
import ai.lzy.v1.slots.LzySlotsApiGrpc;
import com.google.protobuf.Any;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


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

    public SlotsService(String agentId, SlotsManager slotsManager, @Nullable LzyFSManager fsManager) {
        this.agentId = agentId;
        this.slotsManager = slotsManager;
        this.fsManager = fsManager;

        this.longrunningExecutor = Executors.newCachedThreadPool(new ThreadFactory() {
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
                var op = new Operation(
                    agentId,
                    "ConnectSlot: %s -> %s".formatted(fromSlot.shortDesc(), toSlot.shortDesc()),
                    Any.pack(LSA.ConnectSlotMetadata.getDefaultInstance())
                );

                operations.put(op.id(), op);

                longrunningExecutor.submit(() -> {
                    LOG.info("Trying to connect slots, {} -> {}...", fromSlot.shortDesc(), toSlot.shortDesc());
                    try {
                        var dataProvider =  SlotConnectionManager.connectToSlot(toSlot, 0);
                        inputSlot.connect(toSlot.uri(), dataProvider);
                        synchronized (op) {
                            op.setResponse(Any.pack(LSA.ConnectSlotResponse.getDefaultInstance()));
                        }
                        LOG.info("... connected");
                    } catch (Exception e) {
                        LOG.error("Cannot connect slots, {} -> {}: {}",
                            fromSlot.shortDesc(), toSlot.shortDesc(), e.getMessage(), e);
                        synchronized (op) {
                            op.setError(Status.INTERNAL.withDescription(e.getMessage()));
                        }
                    }
                });

                response.onNext(op.toProto());
                response.onCompleted();
            } else {
                var msg = "Slot " + fromSlot.spec().name() + " not found in " + fromSlot.taskId();
                LOG.error(msg);
                response.onError(Status.NOT_FOUND.withDescription(msg).asException());
            }
        }

        @Override
        public void disconnectSlot(LSA.DisconnectSlotRequest request,
                                   StreamObserver<LSA.DisconnectSlotResponse> response)
        {
            LOG.info("LzySlotsApi::disconnectSlot `{}`: {}.",
                request.getSlotInstance().getSlotUri(), JsonUtils.printSingleLine(request));

            final SlotInstance slotInstance = ProtoConverter.fromProto(request.getSlotInstance());

            final LzySlot slot = slotsManager.slot(slotInstance.taskId(), slotInstance.name());
            if (slot == null) {
                var msg = "Slot `" + slotInstance.shortDesc() + "` not found.";
                LOG.error(msg);
                response.onError(Status.NOT_FOUND.withDescription(msg).asException());
                return;
            }

            longrunningExecutor.submit(slot::suspend);

            response.onNext(LSA.DisconnectSlotResponse.getDefaultInstance());
            response.onCompleted();
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
                LOG.error(msg);
                response.onError(Status.NOT_FOUND.withDescription(msg).asException());
                return;
            }

            longrunningExecutor.submit(() -> {
                LOG.info("Explicitly closing slot {}", slotInstance.shortDesc());
                slot.destroy();
                if (fsManager != null) {
                    fsManager.removeSlot(slot.name());
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
}
