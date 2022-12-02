package ai.lzy.channelmanager.v2.operation.action;

import ai.lzy.channelmanager.db.ChannelManagerDataSource;
import ai.lzy.channelmanager.lock.GrainedLock;
import ai.lzy.channelmanager.v2.control.ChannelController;
import ai.lzy.channelmanager.v2.db.ChannelDao;
import ai.lzy.channelmanager.v2.exceptions.ChannelGraphStateException;
import ai.lzy.channelmanager.v2.model.Connection;
import ai.lzy.channelmanager.v2.model.Endpoint;
import ai.lzy.channelmanager.v2.operation.ChannelOperationDao;
import ai.lzy.channelmanager.v2.slot.SlotApiClient;
import ai.lzy.channelmanager.v2.slot.SlotConnectionManager;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.NotFoundException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.Status;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nullable;

import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.util.grpc.ProtoConverter.toProto;

public abstract class ChannelAction implements Runnable {

    private static final Logger LOG = LogManager.getLogger(ChannelAction.class);

    private final ObjectMapper objectMapper;

    protected final String operationId;
    protected final ChannelManagerDataSource storage;
    protected final ChannelDao channelDao;
    protected final OperationDao operationDao;
    protected final ChannelOperationDao channelOperationDao;
    protected final SlotApiClient slotApiClient;
    protected final ChannelController channelController;
    protected final SlotConnectionManager slotConnectionManager;
    protected final GrainedLock lockManager;

    protected ChannelAction(ObjectMapper objectMapper, String operationId, ChannelManagerDataSource storage,
                            ChannelDao channelDao, OperationDao operationDao, ChannelOperationDao channelOperationDao,
                            SlotApiClient slotApiClient, ChannelController channelController,
                            SlotConnectionManager slotConnectionManager, GrainedLock lockManager)
    {
        this.objectMapper = objectMapper;
        this.operationId = operationId;
        this.storage = storage;
        this.channelDao = channelDao;
        this.operationDao = operationDao;
        this.channelOperationDao = channelOperationDao;
        this.slotApiClient = slotApiClient;
        this.channelController = channelController;
        this.slotConnectionManager = slotConnectionManager;
        this.lockManager = lockManager;
    }

    protected void unbindSender(Endpoint sender) throws ChannelGraphStateException {
        final String channelId = sender.getChannelId();
        while (true) {
            final Endpoint receiverToUnbind;
            try (final var guard = lockManager.withLock(sender.getChannelId())) {
                receiverToUnbind = channelController.findReceiverToUnbind(sender);
                if (receiverToUnbind == null) {
                    break;
                }
            }

            LOG.warn("[unbindSender], found bound receiver, need force unbind, sender={}, receiver={}",
                sender.getUri(), receiverToUnbind.getUri());
            unbindReceiver(receiverToUnbind);
        }

        slotApiClient.disconnect(sender);

        // TODO (lindvv): maybe should suspend, so shouldn't destroy, fix after slots refactoring
        slotApiClient.destroy(sender);
        sender.invalidate();

        try (final var guard = lockManager.withLock(channelId)) {
            withRetries(LOG, () -> channelDao.removeEndpointWithoutConnections(sender.getUri().toString(), null));
        } catch (NotFoundException e) {
            LOG.info("[unbindSender] removing endpoint skipped, sender already removed, sender={}", sender.getUri());
        } catch (Exception e) {
            throw new RuntimeException("Failed to remove endpoint in storage");
        }
    }

    protected void unbindReceiver(Endpoint receiver) throws ChannelGraphStateException {
        final String channelId = receiver.getChannelId();

        final Connection connectionToBreak;
        try (final var guard = lockManager.withLock(channelId)) {
            connectionToBreak = channelController.findConnectionToBreak(receiver);
            if (connectionToBreak == null) {
                try {
                    withRetries(LOG, () ->
                        channelDao.removeEndpointWithoutConnections(receiver.getUri().toString(), null));
                } catch (Exception e) {
                    throw new RuntimeException("Failed to remove endpoint in storage");
                }
                return;
            }
        }

        slotApiClient.disconnect(receiver);

        // TODO (lindvv): maybe should suspend, so shouldn't destroy, fix after slots refactoring
        slotApiClient.destroy(receiver);
        receiver.invalidate();

        try (final var guard = lockManager.withLock(channelId)) {
            boolean connectionRemoved = channelController.removeConnection(connectionToBreak.receiver(), connectionToBreak.sender());
            if (connectionRemoved) {
                try {
                    withRetries(LOG, () ->
                        channelDao.removeEndpointWithoutConnections(receiver.getUri().toString(), null));
                } /* TODO can retry scheduled execution in cases of sql exceptions */
                catch (Exception e) {
                    throw new RuntimeException("Failed to remove endpoint in storage");
                }
            }
        }

    }

    protected void updateChannelOperation(String channelId, String stateJson,
                                          @Nullable ChannelDao.UpdateRequest update) throws Exception
    {
        if (update == null) {
            withRetries(LOG, () -> channelOperationDao.update(operationId, stateJson, null));
            return;
        }
        withRetries(LOG, () -> {
            try (final var tx = TransactionHandle.create(storage)) {
                update.run(tx);
                channelOperationDao.update(operationId, stateJson, tx);
                tx.commit();
            }
        });
    }

    protected updateChannelOperation(String channelId, String stateJson, @Nullable ChannelDao.UpdateRequest update) {

        withRetries(LOG, () -> {
            try (var tx = TransactionHandle.create(storage)) {
                channelDao.removeEndpointConnection(
                    state.channelId(),
                    potentialConnection.sender().getUri().toString(),
                    potentialConnection.receiver().getUri().toString(),
                    tx);
                channelOperationDao.update(operationId, stateJson, tx);
            }
        });
    }

    protected void failOperation(Status status) {
        try {
            withRetries(LOG, () -> {
                try (var tx = TransactionHandle.create(storage)) {
                    channelOperationDao.delete(operationId, tx);
                    var operation = operationDao.updateError(operationId, toProto(status).toByteArray(), tx);
                    if (operation == null) {
                        LOG.error("Cannot fail operation {} with reason {}: operation not found",
                            operationId, status.getDescription());
                    } else {
                        tx.commit();
                    }
                }
            });
        } catch (Exception ex) {
            LOG.error("Cannot fail operation {} with reason {}: {}",
                operationId, status.getDescription(), ex.getMessage());
        }
        // TODO if internal
    }

    protected String toJson(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    protected void restart();

}
