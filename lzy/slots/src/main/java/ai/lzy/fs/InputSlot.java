package ai.lzy.fs;

import ai.lzy.fs.backends.InputSlotBackend;
import ai.lzy.fs.transfers.InputTransfer;
import ai.lzy.util.grpc.GrpcUtils;
import ai.lzy.v1.channel.LCMS;
import ai.lzy.v1.common.LC;
import ai.lzy.v1.slots.LSA;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.channels.SeekableByteChannel;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

public class InputSlot extends Thread implements Slot, SlotInternal {
    private static final Logger LOG = LogManager.getLogger(InputSlot.class);
    private static final ThreadGroup INPUT_SLOT_GROUP = new ThreadGroup("InputSlot");

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
        super(INPUT_SLOT_GROUP, "input-slot-%s ".formatted(slotId));

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

            throw new IllegalStateException("Transfer is already started in this slot");
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
    public void close() {
        if (state.getAndSet(State.CLOSED) == State.CLOSED) {
            return;
        }

        this.interrupt();
    }

    public enum State {
        BINDING,  // Binding slot to channel
        WAITING_FOR_PEER,  // Bind call does not have peer, waiting for it
        DOWNLOADING,  // Loading data from peer
        READY,  // Data is ready to be read from backend
        CLOSED  // Slot is closed, by error or from outside
    }

    private void runImpl() throws Exception {
        context.slotsService().register(this); // Registering here before bind
        LOG.info("{} Registered in slots service, binding", logPrefix);

        var resp = bind();

        if (resp == null || state.get() == State.CLOSED) {
            LOG.error("{} Bind failed or slot already closed, failing", logPrefix);
            return;  // run method will clear all resources
        }

        final String transferId;
        final LC.PeerDescription peerDescription;

        if (!resp.hasPeer()) {
            LOG.info("{} Bind call does not have peer, waiting for it", logPrefix);
            state.set(State.WAITING_FOR_PEER);
            var req = waitForPeer.get();
            transferId = req.transferId;
            peerDescription = req.peerDescription;
        } else {
            transferId = resp.getTransferId();
            peerDescription = resp.getPeer();
        }

        LOG.info("{} Got peer {} with transfer {}, starting download", logPrefix, peerDescription, transferId);

        state.set(State.DOWNLOADING);
        context.slotsService().unregister(this.slotId); // Got peer, unregistering

        download(peerDescription, transferId);
        state.set(State.READY);
        ready.complete(null);

        LOG.info("{} Download finished, slot is ready", logPrefix);

        // Creating new output slot for this channel
        var outputBackend = backend.toOutput();
        var slot = new OutputSlot(outputBackend, slotId + "-out", channelId, context);
        context.executionContext().add(slot);
    }

    /**
     * returns null if slot was closed
     */
    @Nullable
    private LCMS.BindResponse bind() throws Exception {
        return GrpcUtils.withRetries(LOG, () -> {
            if (state.get().equals(State.CLOSED)) {
                return null;
            }

            return context.channelManager().bind(LCMS.BindRequest.newBuilder()
                .setPeerId(slotId)
                .setExecutionId(context.executionId())
                .setChannelId(channelId)
                .setPeerUrl(context.apiUrl())
                .setRole(LCMS.BindRequest.Role.CONSUMER)
                .build());
        });
    }

    private void clear() {
        try {
            backend.close();
        } catch (Exception e) {
            LOG.error("{} Error while closing backend: ", logPrefix, e);
        }

        state.set(State.CLOSED);
        context.slotsService().unregister(this.slotId);

        if (!ready.isDone()) {
            LOG.error("{} Closed before ready", logPrefix);
            ready.completeExceptionally(new IllegalStateException("Closed before ready"));
        }

        try {
            context.channelManager().unbind(LCMS.UnbindRequest.newBuilder()
                .setChannelId(channelId)
                .setPeerId(slotId)
                .build());
        } catch (Exception e) {
            LOG.error("{} Error while unbinding: ", logPrefix, e);
            // Ignoring this error
        }
    }

    @Override
    public void run() {
        try {
            runImpl();
        } catch (Exception e) {
            LOG.error("{} Error while running slot: ", logPrefix, e);
        } finally {
            clear();
        }
    }

    private void download(LC.PeerDescription initPeer, String initTransferId) throws Exception {
        long offset = 0;
        var peer = initPeer;
        var transfer = context.transferFactory().input(peer, offset);
        var transferId = initTransferId;
        var failed = false;
        SeekableByteChannel backendStream = backend.openChannel();

        while (true) {
            if (failed) {
                failed = false;

                // Reopening channel
                backendStream.close();
                backendStream = backend.openChannel();

                transfer.close();
                var newPeerResp = context.channelManager()
                    .transferFailed(LCMS.TransferFailedRequest.newBuilder()
                        .setTransferId(transferId)
                        .setChannelId(channelId)
                        .build());

                if (!newPeerResp.hasNewPeer()) {
                    LOG.error("({}) Cannot get peer from channel manager", logPrefix);

                    throw new IllegalStateException("Cannot get data from any peer");
                }

                peer = newPeerResp.getNewPeer();

                LOG.info("({}) Got new peer {}", logPrefix, peer.getPeerId());
                transfer = context.transferFactory().input(peer, offset);
                transferId = newPeerResp.getNewTransferId();
            }

            final int read;

            try {
                read = transfer.transferChunkTo(backendStream);
            } catch (InputTransfer.ReadException e) {
                // Some error while reading from peer, marking it as bad
                LOG.error("({}) Error while reading from peer {}: ", logPrefix, peer.getPeerId(), e);
                failed = true;
                continue;
            } catch (Exception e) {
                // Some error on backend side
                // Failing this slot
                LOG.error("({}) Error while reading into backend: ", logPrefix, e);
                throw e;
            }

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
        }
    }

    private record StartTransferRequest(
        String transferId,
        LC.PeerDescription peerDescription
    ) {}
}
