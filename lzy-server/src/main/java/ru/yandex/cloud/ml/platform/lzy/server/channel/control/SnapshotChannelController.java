package ru.yandex.cloud.ml.platform.lzy.server.channel.control;

import io.grpc.StatusRuntimeException;

import java.net.URI;

import org.apache.commons.lang.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.server.channel.ChannelController;
import ru.yandex.cloud.ml.platform.lzy.server.channel.ChannelException;
import ru.yandex.cloud.ml.platform.lzy.server.channel.ChannelGraph;
import ru.yandex.cloud.ml.platform.lzy.server.channel.Endpoint;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;
import yandex.cloud.priv.datasphere.v2.lzy.LzyWhiteboard;
import yandex.cloud.priv.datasphere.v2.lzy.SnapshotApiGrpc;

public class SnapshotChannelController implements ChannelController {

    private static final Logger LOG = LogManager.getLogger(SnapshotChannelController.class);
    private final String entryId;
    private final String snapshotId;
    private final SnapshotApiGrpc.SnapshotApiBlockingStub snapshotApi;
    private final IAM.Auth auth;
    private Status status = Status.UNBOUND;
    private URI storageURI;

    public SnapshotChannelController(String entryId,
        String snapshotId,
        SnapshotApiGrpc.SnapshotApiBlockingStub snapshotApi,
        IAM.Auth auth
    ) {
        LOG.info("Creating SnapshotChannelController: entryId={}, snapshotId={}", entryId, snapshotId);
        this.entryId = entryId;
        this.snapshotId = snapshotId;
        this.snapshotApi = snapshotApi;
        this.auth = auth;
        try {
            snapshotApi.createEntry(
                LzyWhiteboard.CreateEntryCommand.newBuilder()
                    .setSnapshotId(snapshotId)
                    .setEntryId(entryId)
                    .setAuth(auth)
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
    public synchronized void executeBind(ChannelGraph channelGraph, Endpoint slot) throws ChannelException {
        LOG.info("SnapshotChannelController::executeBind, slot: {} to entryId: {}", slot.uri(), entryId);
        try {
            syncEntryState();
        } catch (IllegalStateException e) {
            slot.destroy();
            executeDestroy(channelGraph);
            throw e;
        }
        switch (slot.slot().direction()) {
            case OUTPUT: {
                if (channelGraph.senders().size() != 0) {
                    slot.destroy();  // TODO(artolord) Think about response to servant design
                    throw new ChannelException("Cannot write to already bound entry. Destroying slot " + slot);
                }
                channelGraph.addSender(slot);
                return;
            }
            case INPUT: {
                if (status == Status.ERRORED) {
                    LOG.info("Entry is errored. Destroying slot {}", slot);
                    slot.destroy();
                    return;
                }
                if (status == Status.COMPLETED) {
                    slot.connect(storageURI);
                }
                channelGraph.addReceiver(slot);
                return;
            }
            default:
                throw new NotImplementedException();
        }
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
        switch (slot.slot().direction()) {
            case INPUT: {
                channelGraph.removeReceiver(slot);
                return;
            }
            case OUTPUT: {
                if (status == Status.COMPLETED) {
                    channelGraph.receivers().forEach(s -> s.connect(storageURI));
                    channelGraph.removeSender(slot);
                } else {  // Some error in sender, entry is not finished, destroying all receivers
                    executeDestroy(channelGraph);
                }
                return;
            }
            default:
                throw new NotImplementedException();
        }
    }

    @Override
    public synchronized void executeDestroy(ChannelGraph channelGraph) throws ChannelException {
        LOG.info("SnapshotChannelController::executeDestroy, entryId={}", entryId);
        try {
            syncEntryState();
        } catch (RuntimeException e) {
            LOG.error("Suppressed error in executeDestroy. Continue destroying", e);
        }
        if (status == Status.IN_PROGRESS || status == Status.ERRORED) {
            status = Status.ERRORED;
            snapshotApi.abort(
                LzyWhiteboard.AbortCommand.newBuilder()
                    .setAuth(auth)
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
        LzyWhiteboard.EntryStatusResponse status = snapshotApi.entryStatus(
            LzyWhiteboard.EntryStatusCommand.newBuilder()
                .setSnapshotId(snapshotId)
                .setEntryId(entryId)
                .setAuth(auth)
                .build());
        switch (status.getStatus()) {
            case FINISHED: {
                if (this.status == Status.ERRORED) {
                    throw new IllegalStateException(
                            "Entry already FINISHED, but ChannelController is in illegal state ERRORED");
                }
                this.status = Status.COMPLETED;
                storageURI = URI.create(status.getStorageUri());
                break;
            }
            case ERRORED: {
                if (this.status == Status.COMPLETED) {
                    this.status = Status.ERRORED;
                    throw new IllegalStateException(
                            "Entry ERRORED, but ChannelController is in illegal state COMPLETED");
                }
                this.status = Status.ERRORED;
                break;
            }
            case IN_PROGRESS: {
                this.status = Status.IN_PROGRESS;
                break;
            }
            case CREATED: {
                if (this.status == Status.COMPLETED || this.status == Status.ERRORED) {
                    this.status = Status.ERRORED;
                    throw new IllegalStateException(
                            "Entry CREATED, but ChannelController is already " + this.status.name());
                }
                break;
            }
            default: {
                throw new RuntimeException("Unknown entry status: " + status.getStatus());
            }
        }
    }

    private enum Status {
        UNBOUND, IN_PROGRESS, COMPLETED, ERRORED
    }
}
