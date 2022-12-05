package ai.lzy.channelmanager.v2.operation.action;

import ai.lzy.channelmanager.db.ChannelManagerDataSource;
import ai.lzy.channelmanager.lock.GrainedLock;
import ai.lzy.channelmanager.v2.control.ChannelController;
import ai.lzy.channelmanager.v2.dao.ChannelDao;
import ai.lzy.channelmanager.v2.dao.ChannelOperationDao;
import ai.lzy.channelmanager.v2.exceptions.ChannelGraphStateException;
import ai.lzy.channelmanager.v2.grpc.SlotConnectionManager;
import ai.lzy.channelmanager.v2.model.Channel;
import ai.lzy.channelmanager.v2.model.Connection;
import ai.lzy.channelmanager.v2.model.Endpoint;
import ai.lzy.channelmanager.v2.operation.ChannelOperationExecutor;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.grpc.ProtoConverter;
import ai.lzy.v1.slots.LSA;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.TimeUnit;

import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.util.grpc.ProtoConverter.toProto;

public abstract class ChannelAction implements Runnable {

    private static final Logger LOG = LogManager.getLogger(ChannelAction.class);

    private final ObjectMapper objectMapper;
    private final ChannelOperationExecutor executor;

    protected final String operationId;
    protected final ChannelManagerDataSource storage;
    protected final ChannelDao channelDao;
    protected final OperationDao operationDao;
    protected final ChannelOperationDao channelOperationDao;
    protected final ChannelController channelController;
    protected final SlotConnectionManager slotConnectionManager;
    protected final GrainedLock lockManager;

    protected boolean operationStopped = false;

    protected ChannelAction(String operationId, ObjectMapper objectMapper, ChannelOperationExecutor executor,
                            ChannelManagerDataSource storage, ChannelDao channelDao, OperationDao operationDao,
                            ChannelOperationDao channelOperationDao, ChannelController channelController,
                            SlotConnectionManager slotConnectionManager, GrainedLock lockManager)
    {
        this.operationId = operationId;
        this.objectMapper = objectMapper;
        this.executor = executor;
        this.storage = storage;
        this.channelDao = channelDao;
        this.operationDao = operationDao;
        this.channelOperationDao = channelOperationDao;
        this.channelController = channelController;
        this.slotConnectionManager = slotConnectionManager;
        this.lockManager = lockManager;
    }

    protected void scheduleRestart() {
        operationStopped = true;
        executor.schedule(this, 1, TimeUnit.SECONDS);
    }

    protected void failOperation(Status status) {
        operationStopped = true;
        try {
            withRetries(LOG, () -> {
                try (var tx = TransactionHandle.create(storage)) {
                    channelOperationDao.fail(operationId, status.getDescription(), tx);
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

    protected void unbindSender(Endpoint sender) throws ChannelGraphStateException {
        LOG.info("Async operation (operationId={}): unbind sender {}", operationId, sender.getUri());

        final String channelId = sender.getChannelId();
        while (true) {
            final Endpoint receiverToUnbind;
            try (final var guard = lockManager.withLock(sender.getChannelId())) {
                final Channel channel;
                try {
                    channel = withRetries(LOG, () -> channelDao.findChannel(sender.getChannelId(), null));
                } catch (Exception e) {
                    String errorMessage = "Failed to find channel" + sender.getChannelId()
                                          + " got exception: " + e.getMessage();
                    throw new RuntimeException(errorMessage);
                }
                if (channel == null) {
                    String errorMessage = "Async operation (operationId=" + operationId + ") cancelled,"
                                          + " channel " + sender.getChannelId() + " not found";
                    LOG.error(errorMessage);
                    this.failOperation(Status.CANCELLED.withDescription(errorMessage));
                    return;
                }

                receiverToUnbind = channelController.findReceiverToUnbind(channel, sender);
                if (receiverToUnbind == null) {
                    LOG.info("Async operation (operationId={}): bound receivers not found, unbindingSender={}",
                        operationId, sender.getUri());
                    break;
                } else {
                    try {
                        withRetries(LOG, () ->
                            channelDao.markEndpointUnbinding(receiverToUnbind.getUri().toString(), null));
                    } catch (Exception e) {
                        String errorMessage = "Failed to mark receiver " + receiverToUnbind.getUri() + " unbinding,"
                                              + " got exception: " + e.getMessage();
                        throw new RuntimeException(errorMessage);
                    }
                }
            }

            LOG.info("Async operation (operationId={}): found bound receiver {}, need force unbind, unbindingSender={}",
                operationId, receiverToUnbind.getUri(), sender.getUri());

            this.unbindReceiver(receiverToUnbind);
            if (operationStopped) {
                return;
            }
        }

        disconnect(sender);
        if (operationStopped) {
            return;
        }

        // TODO (lindvv): maybe should suspend, so shouldn't destroy, fix after slots refactoring
        destroySlot(sender);
        if (operationStopped) {
            return;
        }

        try (final var guard = lockManager.withLock(channelId)) {
            withRetries(LOG, () -> channelDao.removeEndpoint(sender.getUri().toString(), null));
            LOG.info("Async operation (operationId={}): sender removed,"
                     + " unbindingSender={}", operationId, sender.getUri());
        } catch (Exception e) {
            String errorMessage = "Failed to remove sender " + sender.getUri() + ", got exception: " + e.getMessage();
            throw new RuntimeException(errorMessage);
        }
    }

    protected void unbindReceiver(Endpoint receiver) throws ChannelGraphStateException {
        LOG.info("Async operation (operationId={}): unbind receiver {}", operationId, receiver.getUri());

        final String channelId = receiver.getChannelId();

        final Endpoint sender;
        try (final var guard = lockManager.withLock(channelId)) {
            final Channel channel;
            try {
                channel = withRetries(LOG, () -> channelDao.findChannel(receiver.getChannelId(), null));
            } catch (Exception e) {
                String errorMessage = "Failed to find channel" + receiver.getChannelId()
                                      + " got exception: " + e.getMessage();
                throw new RuntimeException(errorMessage);
            }
            if (channel == null) {
                String errorMessage = "Async operation (operationId=" + operationId + ") cancelled,"
                                      + " channel " + receiver.getChannelId() + " not found";
                LOG.error(errorMessage);
                this.failOperation(Status.CANCELLED.withDescription(errorMessage));
                return;
            }

            final Connection connectionToBreak = channelController.findConnectionToBreak(channel, receiver);
            if (connectionToBreak == null) {
                try {
                    withRetries(LOG, () ->
                        channelDao.removeEndpoint(receiver.getUri().toString(), null));
                    LOG.info("Async operation (operationId={}): receiver removed, unbindingReceiver={}",
                        operationId, receiver.getUri());
                } catch (Exception e) {
                    String errorMessage = "Failed to remove receiver " + receiver.getUri() + ","
                                          + " got exception: " + e.getMessage();
                    throw new RuntimeException(errorMessage);
                }
                return;
            } else {
                sender = connectionToBreak.sender();
                try {
                    withRetries(LOG, () -> channelDao.markConnectionDisconnecting(channelId,
                        sender.getUri().toString(), receiver.getUri().toString(), null));
                    LOG.info("Async operation (operationId={}): marked connection disconnecting,"
                             + " unbindingReceiver={}, sender={}", operationId, receiver.getUri(), sender.getUri());
                } catch (Exception e) {
                    throw new RuntimeException("Failed to mark connection disconnecting in storage", e);
                }
            }
        }

        disconnect(receiver);
        if (operationStopped) {
            return;
        }

        // TODO (lindvv): maybe should suspend, so shouldn't destroy, fix after slots refactoring
        destroySlot(receiver);
        if (operationStopped) {
            return;
        }

        try (final var guard = lockManager.withLock(channelId)) {
            withRetries(LOG, () -> {
                try (var tx = TransactionHandle.create(storage)) {
                    channelDao.removeConnection(channelId,
                        sender.getUri().toString(),
                        receiver.getUri().toString(),
                        tx);
                    channelDao.removeEndpoint(receiver.getUri().toString(), tx);
                    tx.commit();
                }
            });
            LOG.info("Async operation (operationId={}): receiver removed,"
                     + " unbindingReceiver={}", operationId, receiver.getUri());
        } catch (Exception e) {
            String errorMessage = "Failed to remove receiver " + receiver.getUri() + " with connection,"
                                  + " got exception: " + e.getMessage();
            throw new RuntimeException(errorMessage);
        }
    }

    protected void disconnect(Endpoint endpoint) {
        final var slotApi = slotConnectionManager.getConnection(endpoint.getUri()).slotApiBlockingStub();
        try {
            final var request = LSA.DisconnectSlotRequest.newBuilder()
                .setSlotInstance(ProtoConverter.toProto(endpoint.getSlot()))
                .build();

            slotApi.disconnectSlot(request);
        } catch (StatusRuntimeException e) {
            if (Status.NOT_FOUND.equals(e.getStatus())) {
                LOG.info("Async operation (operationId={}): disconnectSlot request failed, slot not found. "
                         + "Continue action", operationId);
                return;
            } else {
                LOG.error("Async operation (operationId={}): disconnectSlot request failed"
                          + " with code {}: {}. Schedule restart action",
                    operationId, e.getStatus().getCode(), e.getStatus().getDescription());
                scheduleRestart();
                return;
            }
        }
        LOG.info("Async operation (operationId={}): sent disconnectSlot request", operationId);
    }

    protected void destroySlot(Endpoint endpoint) {
        final var slotApi = slotConnectionManager.getConnection(endpoint.getUri()).slotApiBlockingStub();
        try {
            final var request = LSA.DestroySlotRequest.newBuilder()
                .setSlotInstance(ProtoConverter.toProto(endpoint.getSlot()))
                .build();

            slotApi.destroySlot(request);
        } catch (StatusRuntimeException e) {
            if (Status.NOT_FOUND.equals(e.getStatus())) {
                LOG.info("Async operation (operationId={}): destroySlot request failed, slot not found. "
                         + "Continue action", operationId);
                return;
            } else {
                LOG.error("Async operation (operationId={}): destroySlot request failed"
                          + " with code {}: {}. Schedule restart action",
                    operationId, e.getStatus().getCode(), e.getStatus().getDescription());
                scheduleRestart();
                return;
            }
        }
        LOG.info("Async operation (operationId={}): sent destroySlot request", operationId);
    }

}
