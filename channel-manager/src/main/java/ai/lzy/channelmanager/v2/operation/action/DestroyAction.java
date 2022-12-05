package ai.lzy.channelmanager.v2.operation.action;

import ai.lzy.channelmanager.db.ChannelManagerDataSource;
import ai.lzy.channelmanager.lock.GrainedLock;
import ai.lzy.channelmanager.v2.control.ChannelController;
import ai.lzy.channelmanager.v2.dao.ChannelDao;
import ai.lzy.channelmanager.v2.dao.ChannelOperationDao;
import ai.lzy.channelmanager.v2.exceptions.ChannelGraphStateException;
import ai.lzy.channelmanager.v2.grpc.SlotConnectionManager;
import ai.lzy.channelmanager.v2.model.Channel;
import ai.lzy.channelmanager.v2.model.Endpoint;
import ai.lzy.channelmanager.v2.operation.ChannelOperationExecutor;
import ai.lzy.channelmanager.v2.operation.state.DestroyActionState;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.v1.channel.v2.LCMPS;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Any;
import io.grpc.Status;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static ai.lzy.model.db.DbHelper.withRetries;

public class DestroyAction extends ChannelAction {

    private static final Logger LOG = LogManager.getLogger(DestroyAction.class);

    private final DestroyActionState state;

    public DestroyAction(String operationId, DestroyActionState state,
                         ObjectMapper objectMapper, ChannelOperationExecutor executor,
                         ChannelManagerDataSource storage, ChannelDao channelDao, OperationDao operationDao,
                         ChannelOperationDao channelOperationDao, ChannelController channelController,
                         SlotConnectionManager slotConnectionManager, GrainedLock lockManager)
    {
        super(operationId, objectMapper, executor, storage, channelDao, operationDao, channelOperationDao,
            channelController, slotConnectionManager, lockManager);
        this.state = state;
    }

    @Override
    public void run() {
        LOG.info("Async operation (operationId={}) resumed", operationId);
        operationStopped = false;

        while (true) {
            String channelId = state.toDestroyChannels().stream().findFirst().orElse(null);

            if (channelId == null) {
                try {
                    withRetries(LOG, () -> {
                        try (var tx = TransactionHandle.create(storage)) {
                            channelOperationDao.delete(operationId, tx);
                            operationDao.updateResponse(operationId,
                                Any.pack(LCMPS.ChannelDestroyResponse.getDefaultInstance()).toByteArray(), tx);
                            tx.commit();
                        }
                    });
                    LOG.info("Async operation (operationId={}) finished", operationId);
                } catch (Exception e) {
                    LOG.error("Async operation (operationId={}): failed to finish: {}. Schedule restart action",
                        operationId, e.getMessage());
                    scheduleRestart();
                }
                return;
            }

            LOG.info("Async operation (operationId={}): going to destroy channel {}", operationId, channelId);

            try {
                this.destroyChannel(channelId);

                state.setDestroyed(channelId);
                try {
                    withRetries(LOG, () -> {
                        try (var tx = TransactionHandle.create(storage)) {
                            channelOperationDao.update(operationId, toJson(state), tx);
                            channelDao.removeChannel(channelId, tx);
                            tx.commit();
                        }
                    });
                    LOG.info("Async operation (operationId={}): channel {} destroyed", operationId, channelId);
                } catch (Exception e) {
                    state.unsetDestroyed(channelId);
                    LOG.error("Async operation (operationId={}): failed mark channel {} destroyed: {}. Try later",
                        operationId, channelId, e.getMessage());
                }

            } catch (Exception e) {
                String errorMessage = "Async operation (operationId=" + operationId + ") failed: " + e.getMessage();
                LOG.error(errorMessage);
                this.failOperation(Status.INTERNAL.withDescription(errorMessage));
            }

        }
    }

    private void destroyChannel(String channelId) throws ChannelGraphStateException {
        Channel channel;
        try (final var guard = lockManager.withLock(channelId)) {
            try {
                channel = withRetries(LOG, () -> channelDao.findChannel(channelId, null));
            } catch (Exception e) {
                String errorMessage = "Failed to find channel " + channelId + ","
                                      + " got exception: " + e.getMessage();
                throw new RuntimeException(errorMessage);
            }

            if (channel == null) {
                LOG.info("Async operation (operationId={}): channel {} not found", operationId, channelId);
                return;
            }

            if (channel.getLifeStatus() != Channel.LifeStatus.DESTROYING) {
                try {
                    withRetries(LOG, () -> channelDao.markChannelDestroying(channelId, null));
                } catch (Exception e) {
                    String errorMessage = "Failed to mark channel " + channelId + " destroying,"
                                          + " got exception: " + e.getMessage();
                    throw new RuntimeException(errorMessage);
                }
            }

            try {
                withRetries(LOG, () -> channelDao.markAllEndpointsUnbinding(channelId, null));
            } catch (Exception e) {
                String errorMessage = "Failed to mark endpoints of channel " + channelId + " unbinding,"
                                      + " got exception: " + e.getMessage();
                throw new RuntimeException(errorMessage);
            }
        }

        for (Endpoint receiver : channel.getActiveReceivers().asList()) {
            this.unbindReceiver(receiver);
        }

        for (Endpoint sender : channel.getActiveSenders().asList()) {
            this.unbindSender(sender);
        }

    }
}
