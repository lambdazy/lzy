package ru.yandex.cloud.ml.platform.lzy.server.channel.control;

import io.grpc.StatusRuntimeException;

import java.net.URI;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
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
    private final Lock lock = new ReentrantLock();
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
                if (isCompleted()) {
                    status = Status.COMPLETED;
                }
            } else {
                throw e;
            }
        }
    }

    @Override
    public void executeBind(ChannelGraph channelGraph, Endpoint slot) throws ChannelException {
        LOG.info("SnapshotChannelController::executeBind, slot: {} to entryId: {}", slot.uri(), entryId);
        lock.lock();
        try {
            switch (slot.slot().direction()) {
                case OUTPUT: {
                    if (status != Status.UNBOUND) {
                        slot.destroy();  // TODO(artolord) Think about response to servant design
                        throw new ChannelException("Cannot write to already bound entry. Destroying slot " + slot);
                    }
                    status = Status.IN_PROGRESS;
                    slot.snapshot(snapshotId, entryId);
                    channelGraph.addSender(slot);
                    return;
                }
                case INPUT: {
                    if (status == Status.ERRORED) {
                        LOG.info("Entry is errored. Destroying slot {}", slot);
                        slot.destroy();
                        return;
                    }
                    if (isCompleted()) {
                        status = Status.COMPLETED;
                        slot.connect(storageURI);
                    }
                    channelGraph.addReceiver(slot);
                    return;
                }
                default:
                    throw new NotImplementedException();
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void executeUnBind(ChannelGraph channelGraph, Endpoint slot) throws ChannelException {
        LOG.info("SnapshotChannelController::executeUnBind {}, entryId={}", slot.uri(), entryId);
        lock.lock();
        try {
            switch (slot.slot().direction()) {
                case INPUT: {
                    channelGraph.removeReceiver(slot);
                    return;
                }
                case OUTPUT: {
                    if (status == Status.IN_PROGRESS) {
                        if (isCompleted()) {  // Entry is finished, notifying all receivers
                            this.status = Status.COMPLETED;
                            channelGraph.receivers().forEach(s -> s.connect(storageURI));
                        } else {  // Some error in sender, entry is not finished, destroying all receivers
                            status = Status.ERRORED;
                            channelGraph.receivers().forEach(channelGraph::removeReceiver);
                            snapshotApi.commit(
                                LzyWhiteboard.CommitCommand.newBuilder()
                                    .setAuth(auth)
                                    .setSnapshotId(snapshotId)
                                    .setEntryId(entryId)
                                    .setEmpty(true)
                                    .setErrored(true)
                                    .build()
                            );
                        }
                    }
                    channelGraph.removeSender(slot);
                    return;
                }
                default:
                    throw new NotImplementedException();
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void executeDestroy(ChannelGraph channelGraph) throws ChannelException {
        LOG.info("SnapshotChannelController::executeDestroy, entryId={}", entryId);
        lock.lock();
        try {
            if (status == Status.IN_PROGRESS) {
                status = Status.ERRORED;
                snapshotApi.commit(
                    LzyWhiteboard.CommitCommand.newBuilder()
                        .setAuth(auth)
                        .setSnapshotId(snapshotId)
                        .setEntryId(entryId)
                        .setEmpty(true)
                        .setErrored(true)
                        .build()
                );
            }
            channelGraph.receivers().forEach(Endpoint::destroy);
            channelGraph.senders().forEach(Endpoint::destroy);
        } finally {
            lock.unlock();
        }
    }

    private boolean isCompleted() {
        if (status == Status.COMPLETED) {
            return true;
        }
        LzyWhiteboard.EntryStatusResponse status = snapshotApi.entryStatus(
            LzyWhiteboard.EntryStatusCommand.newBuilder()
                .setSnapshotId(snapshotId)
                .setEntryId(entryId)
                .setAuth(auth)
                .build());
        if (status.getStatus() == LzyWhiteboard.EntryStatusResponse.Status.FINISHED) {
            storageURI = URI.create(status.getStorageUri());
        }
        return status.getStatus() == LzyWhiteboard.EntryStatusResponse.Status.FINISHED;
    }

    private enum Status {
        UNBOUND, IN_PROGRESS, COMPLETED, ERRORED
    }
}
