package ai.lzy.fs;

import ai.lzy.fs.backands.InputSlotBackend;
import ai.lzy.v1.channel.v2.LCMS;
import ai.lzy.v1.common.LC;
import ai.lzy.v1.slots.v2.LSA;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.channels.SeekableByteChannel;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

public class InputSlot extends Thread implements Slot {
    private static final Logger LOG = LogManager.getLogger(InputSlot.class);

    private final InputSlotBackend backend;
    private final String slotId;
    private final String channelId;
    private final String logPrefix;

    private final SlotsContext context;
    private final AtomicReference<State> state = new AtomicReference<>(State.BINDING);

    private final CompletableFuture<StartTransferRequest> waitForPeer = new CompletableFuture<>();
    private final CompletableFuture<Void> ready = new CompletableFuture<>();

    public InputSlot(InputSlotBackend backend, String slotId, String channelId,
                     SlotsContext context)
    {
        this.backend = backend;
        this.slotId = slotId;
        this.channelId = channelId;
        this.context = context;

        this.logPrefix = "InputSlot(slotId: %s, channelId: %s) ".formatted(slotId, channelId);
        this.start();
    }

    @Override
    public CompletableFuture<Void> beforeExecution() {
        return ready;
    }

    @Override
    public CompletableFuture<Void> afterExecution() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public synchronized void startTransfer(LC.PeerDescription peer, String transferId) {
        if (waitForPeer.isDone()) {
            LOG.error("{} Transfer is already started in this slot", logPrefix);

            throw Status.FAILED_PRECONDITION.asRuntimeException();
        }

        var req = new StartTransferRequest(transferId, peer);
        waitForPeer.complete(req);
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
    public synchronized void close() {
        if (state.get().equals(State.CLOSED)) {
            return;
        }

        if (!ready.isDone()) {
            LOG.error("{} Closed before ready", logPrefix);
            ready.completeExceptionally(new IllegalStateException("Closed before ready"));
        }

        setState(State.CLOSED);
        context.slotsService().unregister(this.slotId);

        try {
            context.channelManager().unbind(LCMS.UnbindRequest.newBuilder()
                .setChannelId(channelId)
                .setPeerId(slotId)
                .build());
        } catch (Exception e) {
            LOG.error("{} Error while unbinding: ", logPrefix, e);
            // Ignoring this error
        }

        if (Thread.currentThread().equals(this)) {
            // Not joining to itself
            return;
        }

        this.interrupt();

        try {
            this.join(100);
        } catch (Exception e) {
            LOG.error("{} Error while joining: ", logPrefix, e);
        }
    }

    public enum State {
        BINDING,  // Binding slot to channel
        WAITING_FOR_PEER,  // Bind call does not have peer, waiting for it
        DOWNLOADING,  // Loading data from peer
        READY,  // Data is ready to be read from backand
        CLOSED  // Slot is closed, by error or from outside
    }

    private void logic() throws Exception {
        context.slotsService().register(this); // Registering here before bind

        var resp = context.channelManager().bind(LCMS.BindRequest.newBuilder()
            .setPeerId(slotId)
            .setExecutionId(context.executionId())
            .setChannelId(channelId)
            .setPeerUrl(context.apiUrl())
            .setRole(LCMS.BindRequest.Role.CONSUMER)
            .build());

        final String transferId;
        final LC.PeerDescription peerDescription;

        if (!resp.hasPeer()) {
            setState(State.WAITING_FOR_PEER);
            var req = waitForPeer.get();
            transferId = req.transferId;
            peerDescription = req.peerDescription;
        } else {
            transferId = resp.getTransferId();
            peerDescription = resp.getPeer();
        }

        setState(State.DOWNLOADING);
        context.slotsService().unregister(this.slotId); // Got peer, unregistering

        download(peerDescription, transferId);
        setState(State.READY);
        ready.complete(null);

        // Creating new output slot for this channel
        var outputBackand = backend.toOutput();
        var slot = new OutputSlot(outputBackand, slotId + "-out", channelId, context);
        context.executionContext().addSlot(slot);

        close();
    }

    @Override
    public void run() {
        try {
            logic();
        } catch (Exception e) {
            LOG.error("{} Error while running slot: ", logPrefix, e);
            close();
        }
    }

    private synchronized void setState(State state) {
        this.state.set(state);
        this.notifyAll();
    }

    private void download(LC.PeerDescription initPeer, String initTransferId) throws Exception {
        int offset = 0;
        var peer = initPeer;
        var transfer = context.transferFactory().input(peer, offset);
        var transferId = initTransferId;
        SeekableByteChannel backendStream = null;

        while (true) {
            try {
                if (backendStream == null) {  // reopening stream, if closed
                    backendStream = backend.openChannel();
                }

                var read = transfer.readInto(backendStream);

                if (read == -1) {
                    transfer.close();
                    backendStream.close();
                    context.channelManager().transferCompleted(LCMS.TransferCompletedRequest.newBuilder()
                        .setTransferId(transferId)
                        .setChannelId(channelId)
                        .build());
                    return;

                } else {
                    offset += read;
                }
            } catch (Exception e) {
                LOG.error("Cannot complete transfer to {}: ", peer.getPeerId(), e);
                // Closing old stream. It will be reopened on next iteration
                if (backendStream != null) {
                    backendStream.close();
                }
                backendStream = null;

                transfer.close();

                var newPeerResp = context.channelManager()
                    .transferFailed(LCMS.TransferFailedRequest.newBuilder()
                        .setTransferId(transferId)
                        .setChannelId(channelId)
                        .build());

                if (!newPeerResp.hasNewPeer()) {
                    LOG.error("Cannot get data from any peer");

                    throw Status.INTERNAL
                        .withDescription("Cannot get data from any peer")
                        .asException();
                }

                peer = newPeerResp.getNewPeer();

                transfer = context.transferFactory().input(peer, offset);
                transferId = newPeerResp.getNewTransferId();

            }
        }
    }

    private record StartTransferRequest(
        String transferId,
        LC.PeerDescription peerDescription
    ) {}
}
