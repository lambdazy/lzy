package ai.lzy.fs;

import ai.lzy.fs.backends.OutputSlotBackend;
import ai.lzy.v1.channel.v2.LCMS;
import ai.lzy.v1.common.LC;
import ai.lzy.v1.slots.v2.LSA;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class OutputSlot extends Thread implements Slot, ExecutionCompanion {
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
        super(OUTPUT_SLOTS_TG, "OutputSlot(slotId: %s, channelId: %s)".formatted(slotId, channelId));

        this.backend = backend;
        this.slotId = slotId;
        this.channelId = channelId;
        this.context = context;

        this.logPrefix = "OutputSlot(slotId: %s, channelId: %s)".formatted(slotId, channelId);

        this.start();
    }


    @Override
    public void startTransfer(LC.PeerDescription peer, String transferId) {
        throw Status.UNIMPLEMENTED
            .withDescription("Start transfer is not supported for output slot")
            .asRuntimeException();
    }

    @Override
    public void read(long offset, StreamObserver<LSA.ReadDataChunk> responseObserver) {
        var thread = new ReadThread(offset, responseObserver);
        thread.start();

        runningThreads.add(thread);
    }

    @Override
    public String id() {
        return slotId;
    }

    @Override
    public void beforeExecution() {}

    @Override
    public void afterExecution() throws Exception {
        completeFuture.get();
    }

    @Override
    public void run() {
        try {
            backend.waitCompleted();

            context.slotsService().register(OutputSlot.this);  // Enabling request from outside
            var res = context.channelManager().bind(LCMS.BindRequest.newBuilder()
                .setRole(LCMS.BindRequest.Role.PRODUCER)
                .setPeerId(slotId)
                .setExecutionId(context.executionId())
                .setChannelId(channelId)
                .setPeerUrl(context.apiUrl())
                .build());

            if (res.hasPeer()) {
                try (var transfer = context.transferFactory().output(res.getPeer())) {
                    transfer.readFrom(backend.readFromOffset(0));

                    context.channelManager().transferCompleted(LCMS.TransferCompletedRequest.newBuilder()
                        .setTransferId(res.getTransferId())
                        .setChannelId(channelId)
                        .build());
                } catch (Exception e) {
                    LOG.error("{} Error while transferring data to peer {}: ", logPrefix,
                        res.getPeer().getPeerId(), e);

                    context.channelManager().transferFailed(LCMS.TransferFailedRequest.newBuilder()
                        .setTransferId(res.getTransferId())
                        .setChannelId(channelId)
                        .build());

                    throw e;
                }

                completeFuture.complete(null);
            }


        } catch (Exception e) {
            LOG.error("{} Error while binding output slot: ", logPrefix, e);
            close();
        }
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
                var source = backend.readFromOffset(offset);

                var buffer = ByteBuffer.allocate(4096); // 4KB
                var channel = Channels.newChannel(source);

                while (channel.read(buffer) != -1) {
                    var chunk = LSA.ReadDataChunk.newBuilder()
                        .setChunk(ByteString.copyFrom(buffer.flip()))
                        .build();

                    responseObserver.onNext(chunk);

                    buffer.clear();
                }

                responseObserver.onNext(LSA.ReadDataChunk
                    .newBuilder()
                    .setControl(LSA.ReadDataChunk.Control.EOS)
                    .build());
                responseObserver.onCompleted();
            } catch (Exception e) {
                responseObserver.onError(e);
            }
        }
    }

    @Override
    public void close() {
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

        if (Thread.currentThread().equals(this)) {
            return;  // Not joining itself
        }

        this.interrupt();
        try {
            this.join(100);
        } catch (InterruptedException e) {
            LOG.error("{} Error while waiting for thread to join: ", logPrefix, e);
        }
    }
}
