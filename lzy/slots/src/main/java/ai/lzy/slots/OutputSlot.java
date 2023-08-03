package ai.lzy.slots;

import ai.lzy.slots.backends.OutputSlotBackend;
import ai.lzy.util.grpc.ContextAwareTask;
import ai.lzy.v1.channel.LCMS;
import ai.lzy.v1.common.LC;
import ai.lzy.v1.slots.LSA;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static ai.lzy.util.grpc.GrpcUtils.LONGENOUGH_RETRY_CONFIG;
import static ai.lzy.util.grpc.GrpcUtils.withIdempotencyKey;
import static ai.lzy.util.grpc.GrpcUtils.withRetries;

public class OutputSlot implements Slot, SlotInternal {
    private static final ThreadGroup OUTPUT_SLOTS_TG = new ThreadGroup("OutputSlots");
    private static final Logger LOG = LogManager.getLogger(OutputSlot.class);

    private final OutputSlotBackend backend;
    private final String slotId;
    private final String channelId;
    private final CompletableFuture<Void> completeFuture = new CompletableFuture<>();
    private final SlotsContext context;
    private final List<Thread> runningThreads = Collections.synchronizedList(new ArrayList<>());
    private final String logPrefix;

    public OutputSlot(OutputSlotBackend backend, String slotId, String channelId,
                      SlotsContext context)
    {
        this.backend = backend;
        this.slotId = slotId;
        this.channelId = channelId;
        this.context = context;

        this.logPrefix = "OutputSlot(slotId: %s, channelId: %s)".formatted(slotId, channelId);

        var thread = new PrepareThread();
        thread.start();

        runningThreads.add(thread);
    }

    @Override
    public void startTransfer(LC.PeerDescription peer, String transferId) {
        throw new NotImplementedException("Cannot start transfer in output slot");
    }

    @Override
    public void read(long offset, StreamObserver<LSA.ReadDataChunk> responseObserver) {
        LOG.info("{} Read request with offset {}", logPrefix, offset);
        var thread = new ReadThread(offset, responseObserver);
        thread.start();

        runningThreads.add(thread);
    }

    @Override
    public String id() {
        return slotId;
    }

    @Override
    public CompletableFuture<Void> beforeExecution() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> afterExecution() {
        return completeFuture;
    }

    @Override
    public void close() {
        LOG.info("{} close", logPrefix);
        // Failed, so no more requests from outside available
        context.slotsService().unregister(slotId);

        if (!completeFuture.isDone()) {
            completeFuture.completeExceptionally(new RuntimeException("Slot closed before ready"));
        }

        try {
            var stub = withIdempotencyKey(context.channelManager(), UUID.randomUUID().toString());
            withRetries(LOG, LONGENOUGH_RETRY_CONFIG, () -> stub.unbind(
                LCMS.UnbindRequest.newBuilder()
                    .setChannelId(channelId)
                    .setPeerId(slotId)
                    .build()));
        } catch (StatusRuntimeException e) {
            LOG.error("{} Error while unbinding output slot: {}", logPrefix, e.getStatus());
            // Error ignored
        }

        try {
            backend.close();
        } catch (Exception e) {
            LOG.error("{} Error while closing backend for output slot: {}", logPrefix, e.getMessage());
            // Error ignored
        }

        LOG.info("{} All resources cleared, waiting for running threads", logPrefix);

        runningThreads.forEach(Thread::interrupt);

        runningThreads.forEach(thread -> {
            try {
                thread.join(100);
            } catch (InterruptedException e) {
                LOG.error("{} Error while waiting for thread to join: ", logPrefix, e);
            }
            thread.stop();  // Force stop if not closed by interrupt
        });

        LOG.info("{} Closed", logPrefix);
    }

    private class PrepareThread extends Thread {
        private final Runnable body;

        PrepareThread() {
            super(OUTPUT_SLOTS_TG, "prepare-output-slot-%s".formatted(slotId));
            body = new ContextAwareTask() {
                @Override
                protected void execute() {
                    try {
                        runImpl();
                    } catch (Exception e) {
                        LOG.error("{} Error while binding output slot: ", logPrefix, e);
                        completeFuture.completeExceptionally(e);
                    }
                }
            };
        }

        @Override
        public void run() {
            body.run();
        }

        private void runImpl() throws Exception {
            LOG.info("{} Waiting for data", logPrefix);

            backend.waitCompleted();
            context.slotsService().register(OutputSlot.this);  // Enable request from outside

            LOG.info("{} Data ready, bind...", logPrefix);
            var res = bind();

            if (res.hasPeer()) {
                LOG.info("{} Got peer in bind response, start transfer to {}",
                    logPrefix, res.getPeer().getPeerId());
                var transfer = context.transferFactory().output(res.getPeer());

                if (transfer == null) {
                    LOG.error("({}) Cannot create transfer for peer {}", logPrefix, res.getPeer());
                    failTransfer(res.getTransferId());
                    throw new IllegalStateException("Cannot create transfer for peer " + res.getPeer());
                }

                try (transfer) {
                    transfer.readFrom(backend.readFromOffset(0));
                    completeTransfer(res.getTransferId());
                } catch (Exception e) {
                    LOG.error("{} Error while transferring data to peer {}: ", logPrefix,
                        res.getPeer().getPeerId(), e);

                    failTransfer(res.getTransferId());
                    throw e;
                }
            }

            LOG.info("{} Slot is ready", logPrefix);
            completeFuture.complete(null);
        }
    }

    private void failTransfer(String transferId) throws StatusRuntimeException {
        var stub = withIdempotencyKey(context.channelManager(), UUID.randomUUID().toString());
        withRetries(LOG, LONGENOUGH_RETRY_CONFIG, () -> stub.transferFailed(
            LCMS.TransferFailedRequest.newBuilder()
                .setTransferId(transferId)
                .setChannelId(channelId)
                .build()));
    }

    private LCMS.BindResponse bind() throws StatusRuntimeException {
        var stub = withIdempotencyKey(context.channelManager(), UUID.randomUUID().toString());
        return withRetries(LOG, () -> stub.bind(
            LCMS.BindRequest.newBuilder()
                .setRole(LCMS.BindRequest.Role.PRODUCER)
                .setPeerId(slotId)
                .setExecutionId(context.executionId())
                .setChannelId(channelId)
                .setPeerUrl(context.apiUrl())
                .build()));
    }

    private void completeTransfer(String transferId) throws StatusRuntimeException {
        var stub = withIdempotencyKey(context.channelManager(), UUID.randomUUID().toString());
        withRetries(LOG, () -> stub.transferCompleted(
            LCMS.TransferCompletedRequest.newBuilder()
                .setTransferId(transferId)
                .setChannelId(channelId)
                .build()));
    }

    private class ReadThread extends Thread {
        private final Runnable body;

        private ReadThread(long offset, StreamObserver<LSA.ReadDataChunk> responseObserver) {
            super(OUTPUT_SLOTS_TG, "ReadThread(offset: %d, from_slot: %s)".formatted(offset, slotId));

            this.body = new ContextAwareTask() {
                @Override
                protected Map<String, String> prepareLogContext() {
                    return Map.of("tid", "");
                }

                @Override
                protected void execute() {
                    LOG.info("{} Reading from offset {}", logPrefix, offset);
                    try (var source = backend.readFromOffset(offset)) {
                        var buffer = ByteBuffer.allocate(2 << 20L); // 2MB

                        while (source.read(buffer) != -1) {
                            var chunk = LSA.ReadDataChunk.newBuilder()
                                .setChunk(ByteString.copyFrom(buffer.flip()))
                                .build();

                            responseObserver.onNext(chunk);

                            buffer.clear();
                        }

                        LOG.info("{} End of stream", logPrefix);
                    } catch (Exception e) {
                        LOG.error("{} Error while reading from backend: ", logPrefix, e);
                        responseObserver.onError(Status.INTERNAL.asException());
                        return;
                    }

                    responseObserver.onNext(
                        LSA.ReadDataChunk.newBuilder()
                            .setControl(LSA.ReadDataChunk.Control.EOS)
                            .build());
                    responseObserver.onCompleted();
                }
            };
        }

        @Override
        public void run() {
            body.run();
        }
    }
}
