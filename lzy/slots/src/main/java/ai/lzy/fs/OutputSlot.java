package ai.lzy.fs;

import ai.lzy.fs.backends.OutputSlotBackend;
import ai.lzy.util.grpc.GrpcUtils;
import ai.lzy.v1.channel.v2.LCMS;
import ai.lzy.v1.common.LC;
import ai.lzy.v1.slots.v2.LSA;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

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
        LOG.info("{} Read request for offset {}", logPrefix, offset);
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
        LOG.info("{} Closing", logPrefix);
        // Failed, so no more requests from outside available
        context.slotsService().unregister(slotId);

        if (!completeFuture.isDone()) {
            completeFuture.completeExceptionally(new RuntimeException("Slot closed before ready"));
        }

        try {
            context.channelManager().unbind(LCMS.UnbindRequest.newBuilder()
                .setChannelId(channelId)
                .setPeerId(slotId)
                .build());
        } catch (Exception e) {
            LOG.error("{} Error while unbinding output slot: ", logPrefix, e);
            // Error ignored
        }

        try {
            backend.close();
        } catch (Exception e) {
            LOG.error("{} Error while closing backend for output slot: ", logPrefix, e);
            // Error ignored
        }

        LOG.info("{} All resources cleared, waiting for running threads", logPrefix);

        for (var thread : runningThreads) {
            thread.interrupt();
        }

        for (var thread: runningThreads) {
            try {
                thread.join(100);
            } catch (InterruptedException e) {
                LOG.error("{} Error while waiting for thread to join: ", logPrefix, e);
            }

            thread.stop();  // Force stop if not closed by interrupt
        }

        LOG.info("{} Closed", logPrefix);
    }

    private class PrepareThread extends Thread {
        PrepareThread() {
            super(OUTPUT_SLOTS_TG, "prepare-output-slot-%s".formatted(slotId));
        }

        @Override
        public void run() {
            try {
                runImpl();
            } catch (Exception e) {
                LOG.error("{} Error while binding output slot: ", logPrefix, e);
                completeFuture.completeExceptionally(e);
            }
        }

        private void runImpl() throws Exception {
            LOG.info("{} Waiting for data", logPrefix);

            backend.waitCompleted();
            context.slotsService().register(OutputSlot.this);  // Enable request from outside

            LOG.info("{} Data ready, binding", logPrefix);
            var res = bind();

            if (res.hasPeer()) {
                LOG.info("{} Got peer in bind response, starting transfer {}",
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

                completeFuture.complete(null);
            }
        }
    }

    private void failTransfer(String transferId) {
        context.channelManager().transferFailed(LCMS.TransferFailedRequest.newBuilder()
            .setTransferId(transferId)
            .setChannelId(channelId)
            .build());
    }

    private LCMS.BindResponse bind() throws Exception {
        return GrpcUtils.withRetries(LOG, () -> context.channelManager().bind(LCMS.BindRequest.newBuilder()
            .setRole(LCMS.BindRequest.Role.PRODUCER)
            .setPeerId(slotId)
            .setExecutionId(context.executionId())
            .setChannelId(channelId)
            .setPeerUrl(context.apiUrl())
            .build()));
    }

    private void completeTransfer(String transferId) throws Exception {
        GrpcUtils.withRetries(LOG, () -> context.channelManager().transferCompleted(
            LCMS.TransferCompletedRequest.newBuilder()
                .setTransferId(transferId)
                .setChannelId(channelId)
                .build()));
    }

    private class ReadThread extends Thread {
        private final long offset;
        private final StreamObserver<LSA.ReadDataChunk> responseObserver;

        private ReadThread(long offset, StreamObserver<LSA.ReadDataChunk> responseObserver) {
            super(OUTPUT_SLOTS_TG, "ReadThread(offset: %d, from_slot: %s)".formatted(offset, slotId));
            this.offset = offset;
            this.responseObserver = responseObserver;
        }

        @Override
        public void run() {
            try {
                LOG.info("{} Reading from offset {}", logPrefix, offset);
                var source = backend.readFromOffset(offset);

                var buffer = ByteBuffer.allocate(1024 * 1024); // 1MB

                while (source.read(buffer) != -1) {
                    var chunk = LSA.ReadDataChunk.newBuilder()
                        .setChunk(ByteString.copyFrom(buffer.flip()))
                        .build();

                    responseObserver.onNext(chunk);

                    buffer.clear();
                }

                LOG.info("{} End of stream", logPrefix);

                responseObserver.onNext(LSA.ReadDataChunk
                    .newBuilder()
                    .setControl(LSA.ReadDataChunk.Control.EOS)
                    .build());
                responseObserver.onCompleted();
            } catch (Exception e) {
                LOG.error("{} Error while reading from backend: ", logPrefix, e);
                responseObserver.onError(Status.INTERNAL.asException());
            }
        }
    }
}
