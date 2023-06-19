package ai.lzy.fs;

import ai.lzy.fs.backands.InputSlotBackend;
import ai.lzy.fs.transfers.InputTransfer;
import ai.lzy.fs.transfers.OutputTransfer;
import ai.lzy.v1.channel.v2.LCMS;
import ai.lzy.v1.common.LC;
import ai.lzy.v1.slots.v2.LSA;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

public class InputSlot implements Slot {
    private static final Logger LOG = LogManager.getLogger(InputSlot.class);

    private final InputSlotBackend backend;
    private final String slotId;
    private final String channelId;
    private final CompletableFuture<Void> preparingFuture = new CompletableFuture<>();

    private final AtomicReference<Thread> transferThread = new AtomicReference<>(null);
    private final String logPrefix;

    private final SlotsContext context;

    public InputSlot(InputSlotBackend backend, String slotId, String channelId,
                     SlotsContext context)
    {
        this.backend = backend;
        this.slotId = slotId;
        this.channelId = channelId;
        this.context = context;

        this.logPrefix = "InputSlot(slotId: %s, channelId: %s) ".formatted(slotId, channelId);
    }

    @Override
    public CompletableFuture<Void> beforeExecution() throws IOException {
        context.slotsService().register(this); // Registering here before bind

        var resp = context.channelManager().bind(LCMS.BindRequest.newBuilder()
            .setPeerId(slotId)
            .setExecutionId(context.executionId())
            .setChannelId(channelId)
            .setPeerUrl(context.apiUrl())
            .setRole(LCMS.BindRequest.Role.CONSUMER)
            .build());

        if (!resp.hasPeer()) {
            // Waiting for startTransferCall
            return preparingFuture;
        }

        context.slotsService().unregister(this.slotId); // Got peer, unregistering

        var thread = new DownloadThread(resp.getPeer());
        transferThread.set(thread);

        thread.start();

        return preparingFuture;
    }

    @Override
    public CompletableFuture<Void> afterExecution() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public synchronized void startTransfer(LC.PeerDescription peer) {
        if (transferThread.get() != null) {
            LOG.error("{} Transfer is already started in this slot", logPrefix);

            throw Status.FAILED_PRECONDITION.asRuntimeException();
        }

        var thread = new DownloadThread(peer);
        transferThread.set(thread);
        thread.start();

        // Got peer, so no need for other calls to this slot from outside
        context.slotsService().unregister(this.slotId);
    }

    @Override
    public void read(long offset, StreamObserver<LSA.ReadDataChunk> transfer) {
        transfer.onError(Status.UNIMPLEMENTED.asRuntimeException());
    }

    @Override
    public String id() {
        return slotId;
    }

    @Override
    public void fail() {
        context.slotsService().unregister(this.slotId);
        var thread = transferThread.get();

        if (thread != null) {
            thread.interrupt();

            try {
                thread.join();
            } catch (InterruptedException e) {
                LOG.error("{} Interrupted while waiting for transfer thread to finish", logPrefix);
            }
        }
    }

    private class DownloadThread extends Thread {
        private final LC.PeerDescription initPeer;

        private DownloadThread(LC.PeerDescription initPeer) {
            this.initPeer = initPeer;
        }

        @Override
        public void run() {
            boolean done = false;
            int offset = 0;
            var peer = initPeer;
            var transfer = context.transferFactory().input(peer, offset);

            while (!done) {
                try {
                    var read = transfer.readInto(backend.outputStream());

                    if (read == -1) {
                        done = true;

                        context.channelManager().transferCompleted(LCMS.TransferCompletedRequest.newBuilder()
                            .setSlotId(slotId)
                            .setChannelId(channelId)
                            .setPeerId(peer.getPeerId())
                            .build());

                    } else {
                        offset += read;
                    }
                } catch (InputTransfer.ReadException e) {
                    LOG.error("Cannot complete transfer to {}: ", peer.getPeerId(), e);

                    var newPeerResp = context.channelManager()
                        .transferFailed(LCMS.TransferFailedRequest.newBuilder()
                            .setSlotId(slotId)
                            .setChannelId(channelId)
                            .setPeerId(peer.getPeerId())
                            .build());

                    if (!newPeerResp.hasNewPeer()) {
                        LOG.error("Cannot get data from any peer");
                        var ex = Status.INTERNAL
                            .withDescription("Cannot get data from any peer")
                            .asRuntimeException();

                        preparingFuture.completeExceptionally(ex);
                        return;
                    }

                    peer = newPeerResp.getNewPeer();
                    transfer.close();
                    transfer = context.transferFactory().input(peer, offset);
                } catch (Exception e) {  // Unknown exception
                    LOG.error("Cannot complete transfer to {}: ", peer.getPeerId(), e);
                    transfer.close();

                    var ex = Status.INTERNAL
                        .withDescription("Cannot complete transfer to " + peer.getPeerId())
                        .asRuntimeException();

                    preparingFuture.completeExceptionally(ex);
                    return;
                }
            }

            transfer.close();
            preparingFuture.complete(null);

            // Creating new output slot for this channel
            var outputBackand = backend.toOutput();
            var slot = new OutputSlot(outputBackand, slotId + "-out", channelId, context);
            context.executionContext().addSlot(slot);
        }
    }
}
