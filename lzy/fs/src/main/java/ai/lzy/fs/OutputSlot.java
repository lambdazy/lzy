package ai.lzy.fs;

import ai.lzy.fs.backands.OutputSlotBackend;
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

public class OutputSlot implements Slot {
    private static final Logger LOG = LogManager.getLogger(OutputSlot.class);

    private final OutputSlotBackend backend;
    private final String slotId;
    private final String channelId;
    private final CompletableFuture<Void> completeFuture = new CompletableFuture<>();
    private final SlotsContext context;
    private final List<Thread> runningThreads = Collections.synchronizedList(new ArrayList<>());

    public OutputSlot(OutputSlotBackend backend, String slotId, String channelId,
                      SlotsContext context)
    {
        this.backend = backend;
        this.slotId = slotId;
        this.channelId = channelId;
        this.context = context;
    }


    @Override
    public void startTransfer(LC.PeerDescription peer) {
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
    public CompletableFuture<Void> beforeExecution() {
        var thread = new PrepareThread();
        thread.start();
        runningThreads.add(thread);

        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> afterExecution() {
        return completeFuture;
    }

    private class PrepareThread extends Thread {
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
                    try {
                        var transfer = context.transferFactory().output(res.getPeer());
                        transfer.readFrom(backend.readFromOffset(0));

                        context.channelManager().transferCompleted(LCMS.TransferCompletedRequest.newBuilder()
                            .setSlotId(slotId)
                            .setPeerId(res.getPeer().getPeerId())
                            .setChannelId(channelId)
                            .build());
                    } catch (Exception e) {
                        LOG.error("Error while transferring data to peer " + res.getPeer().getPeerId(), e);

                        context.channelManager().transferFailed(LCMS.TransferFailedRequest.newBuilder()
                            .setSlotId(slotId)
                            .setPeerId(res.getPeer().getPeerId())
                            .setChannelId(channelId)
                            .build());

                        completeFuture.completeExceptionally(e);
                        fail();
                        return;
                    }

                    completeFuture.complete(null);
                }


            } catch (Exception e) {
                LOG.error("Error while binding output slot {}", slotId, e);

                completeFuture.completeExceptionally(e);
                fail();
            }
        }
    }

    private class ReadThread extends Thread {
        private final long offset;
        private final StreamObserver<LSA.ReadDataChunk> responseObserver;

        private ReadThread(long offset, StreamObserver<LSA.ReadDataChunk> responseObserver) {
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
    public void fail() {
        // Failed, so no more requests from outside available
        context.slotsService().unregister(slotId);

        for (var thread : runningThreads) {
            thread.interrupt();
        }

        for (var thread: runningThreads) {
            try {
                thread.join(100);
            } catch (InterruptedException e) {
                LOG.error("Error while waiting for thread to join", e);
            }

            thread.stop();
        }

        if (!completeFuture.isDone()) {
            completeFuture.completeExceptionally(new RuntimeException("Slot failed"));
        }
    }
}
