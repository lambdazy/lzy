package ai.lzy.channelmanager.control;

import ai.lzy.channelmanager.graph.ChannelGraph;
import ai.lzy.channelmanager.channel.ChannelException;
import ai.lzy.channelmanager.channel.Endpoint;
import ai.lzy.model.SlotInstance;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.v1.IAM;
import ai.lzy.v1.LzyWhiteboard;
import ai.lzy.v1.SnapshotApiGrpc;
import io.grpc.Channel;
import io.grpc.StatusRuntimeException;
import java.net.URI;
import java.util.stream.Stream;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SnapshotChannelController implements ChannelController {

    private static final Logger LOG = LogManager.getLogger(SnapshotChannelController.class);
    private static SnapshotApiGrpc.SnapshotApiBlockingStub SNAPSHOT_API;

    private final String entryId;
    private final String snapshotId;
    private final IAM.Auth userCredentials;
    private Status status = Status.UNBOUND;
    private URI storageURI;

    public SnapshotChannelController(
        String entryId,
        String snapshotId,
        String userId,
        URI snapshotAddress
    ) {
        LOG.info("Creating SnapshotChannelController: entryId={}, snapshotId={}", entryId, snapshotId);
        synchronized (SnapshotChannelController.class) {
            if (SNAPSHOT_API == null) {
                Channel channel = ChannelBuilder.forAddress(snapshotAddress.getHost(), snapshotAddress.getPort())
                    .enableRetry(SnapshotApiGrpc.SERVICE_NAME)
                    .usePlaintext()
                    .build();
                SNAPSHOT_API = SnapshotApiGrpc.newBlockingStub(channel);
            }
        }
        this.entryId = entryId;
        this.snapshotId = snapshotId;
        this.userCredentials = IAM.Auth.newBuilder().setUser(
                IAM.UserCredentials.newBuilder()
                    .setUserId(userId)
                    .build())
            .build();
        try {
            //noinspection ResultOfMethodCallIgnored
            SNAPSHOT_API.createEntry(
                LzyWhiteboard.CreateEntryCommand.newBuilder()
                    .setAuth(userCredentials)
                    .setSnapshotId(snapshotId)
                    .setEntryId(entryId)
                    .build()
            );
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == io.grpc.Status.ALREADY_EXISTS.getCode()) {
                LOG.info("Entry already exists, using it");
            } else {
                throw e;
            }
        }
        syncEntryState();
    }

    @Override
    public synchronized Stream<Endpoint> executeBind(ChannelGraph channelGraph, Endpoint slot) throws ChannelException {
        LOG.info("SnapshotChannelController::executeBind, slot: {} to entryId: {}", slot.uri(), entryId);
        try {
            syncEntryState();
        } catch (IllegalStateException e) {
            slot.destroy();
            executeDestroy(channelGraph);
            throw e;
        }
        switch (slot.slotSpec().direction()) {
            case OUTPUT -> {
                if (channelGraph.senders().size() != 0) {
                    slot.destroy();  // TODO(artolord) Think about response to servant design
                    throw new ChannelException("Cannot write to already bound entry. Destroying slot " + slot);
                }
                channelGraph.addSender(slot);
            }
            case INPUT -> {
                if (status == Status.ERRORED) {
                    LOG.info("Entry is errored. Destroying slot {}", slot);
                    slot.destroy();
                    return Stream.empty();
                }
                if (status == Status.COMPLETED) {
                    final SlotInstance slotInstance = slot.slotInstance();
                    slot.connect(new SlotInstance(
                        slotInstance.spec(),
                        "unknown_snapshot_task_id",
                        channelGraph.ownerChannelId(),
                        storageURI)
                    );
                }
                channelGraph.addReceiver(slot);
            }
            default -> throw new NotImplementedException("ExecuteBind got unexpected slot direction type");
        }
        return Stream.empty();
    }

    @Override
    public synchronized void executeUnBind(ChannelGraph channelGraph, Endpoint slot) throws ChannelException {
        LOG.info("SnapshotChannelController::executeUnBind {}, entryId={}", slot.uri(), entryId);
        try {
            syncEntryState();
        } catch (IllegalStateException e) {
            slot.destroy();
            executeDestroy(channelGraph);
            throw e;
        }
        switch (slot.slotSpec().direction()) {
            case INPUT -> channelGraph.removeReceiver(slot);
            case OUTPUT -> {
                if (status == Status.COMPLETED) {
                    channelGraph.receivers().forEach(
                        s -> s.connect(new SlotInstance(
                            slot.slotSpec(),
                            "unknown_snapshot_task_id",
                            channelGraph.ownerChannelId(),
                            storageURI))
                    );
                    channelGraph.removeSender(slot);
                } else {  // Some error in sender, entry is not finished, destroying all receivers
                    executeDestroy(channelGraph);
                }
            }
            default -> throw new NotImplementedException("ExecuteUnBind got unexpected slot direction type");
        }
    }

    @Override
    public synchronized void executeDestroy(ChannelGraph channelGraph) {
        LOG.info("SnapshotChannelController::executeDestroy, entryId={}", entryId);
        try {
            syncEntryState();
        } catch (RuntimeException e) {
            LOG.error("Suppressed error in executeDestroy. Continue destroying", e);
        }
        if (status == Status.IN_PROGRESS || status == Status.ERRORED) {
            status = Status.ERRORED;
            //noinspection ResultOfMethodCallIgnored
            SNAPSHOT_API.abort(
                LzyWhiteboard.AbortCommand.newBuilder()
                    .setAuth(userCredentials)
                    .setSnapshotId(snapshotId)
                    .setEntryId(entryId)
                    .build()
            );
        }
        channelGraph.receivers().forEach(Endpoint::destroy);
        channelGraph.senders().forEach(Endpoint::destroy);
    }

    private void syncEntryState() {
        if (status == Status.ERRORED) {
            return;
        }
        LzyWhiteboard.EntryStatusResponse status = SNAPSHOT_API.entryStatus(
            LzyWhiteboard.EntryStatusCommand.newBuilder()
                .setAuth(userCredentials)
                .setSnapshotId(snapshotId)
                .setEntryId(entryId)
                .build());
        switch (status.getStatus()) {
            case FINISHED -> {
                if (this.status == Status.ERRORED) {
                    throw new IllegalStateException(
                        "Entry already FINISHED, but ChannelController is in illegal state ERRORED");
                }
                this.status = Status.COMPLETED;
                storageURI = URI.create(status.getStorageUri());
            }
            case ERRORED -> {
                if (this.status == Status.COMPLETED) {
                    this.status = Status.ERRORED;
                    throw new IllegalStateException(
                        "Entry ERRORED, but ChannelController is in illegal state COMPLETED");
                }
                this.status = Status.ERRORED;
            }
            case IN_PROGRESS -> this.status = Status.IN_PROGRESS;
            case CREATED -> {
                if (this.status == Status.COMPLETED || this.status == Status.ERRORED) {
                    this.status = Status.ERRORED;
                    throw new IllegalStateException(
                        "Entry CREATED, but ChannelController is already " + this.status.name());
                }
            }
            default -> throw new RuntimeException("Unknown entry status: " + status.getStatus());
        }
    }

    private enum Status {
        UNBOUND, IN_PROGRESS, COMPLETED, ERRORED
    }
}
