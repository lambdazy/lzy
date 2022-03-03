package ru.yandex.cloud.ml.platform.lzy.server.channel.control;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
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
    private final AtomicBoolean completed = new AtomicBoolean(false);
    private final AtomicBoolean errored = new AtomicBoolean(false);

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
    }

    @Override
    public void executeBind(ChannelGraph channelGraph, Endpoint slot) throws ChannelException {
        LOG.info("SnapshotChannelController::executeBind {}, entryId={}", slot.uri(), entryId);
        if (slot.slot().direction() == Slot.Direction.OUTPUT) {
            if (isCompleted() || errored.get() || channelGraph.senders().size() > 0) {
                LOG.info("Cannot write to already completed entry. Destroying slot {}", slot);
                slot.destroy();
                return;
            }
            slot.snapshot(snapshotId, entryId);
            channelGraph.addSender(slot);
            return;
        }
        if (errored.get()) {
            LOG.info("Entry is errored. Destroying slot {}", slot);
            slot.destroy();
            return;
        }
        if (isCompleted()) {
            slot.snapshot(snapshotId, entryId);
        }
        channelGraph.addReceiver(slot);
    }

    @Override
    public void executeUnBind(ChannelGraph channelGraph, Endpoint slot) throws ChannelException {
        LOG.info("SnapshotChannelController::executeUnBind {}, entryId={}", slot.uri(), entryId);
        if (slot.slot().direction() == Slot.Direction.OUTPUT) {
            if (!errored.get()) {
                if (isCompleted()) {  // Entry is finished, notifying all receivers
                    channelGraph.receivers().forEach(s -> s.snapshot(snapshotId, entryId));
                } else {  // Some error in sender, entry is not finished, destroying all receivers
                    errored.set(true);
                    channelGraph.receivers().forEach(channelGraph::removeReceiver);
                }
            }
            channelGraph.removeSender(slot);
            return;
        }
        channelGraph.removeReceiver(slot);
    }

    @Override
    public void executeDestroy(ChannelGraph channelGraph) throws ChannelException {
        LOG.info("SnapshotChannelController::executeDestroy, entryId={}", entryId);
        channelGraph.receivers().forEach(Endpoint::destroy);
        channelGraph.senders().forEach(Endpoint::destroy);
    }

    private boolean isCompleted() {
        if (completed.get()) {
            return true;
        }
        LzyWhiteboard.EntryStatusResponse status = snapshotApi.entryStatus(
            LzyWhiteboard.EntryStatusCommand.newBuilder()
                .setSnapshotId(snapshotId)
                .setEntryId(entryId)
                .setAuth(auth)
                .build());
        if (status.getStatus() == LzyWhiteboard.EntryStatusResponse.Status.FINISHED) {
            completed.set(true);
            return true;
        }
        return false;
    }


}
