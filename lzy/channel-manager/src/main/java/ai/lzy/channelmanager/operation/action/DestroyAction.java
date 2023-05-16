package ai.lzy.channelmanager.operation.action;

import ai.lzy.channelmanager.control.ChannelController;
import ai.lzy.channelmanager.dao.ChannelDao;
import ai.lzy.channelmanager.dao.ChannelManagerDataSource;
import ai.lzy.channelmanager.dao.ChannelOperationDao;
import ai.lzy.channelmanager.exceptions.ChannelGraphStateException;
import ai.lzy.channelmanager.grpc.SlotConnectionManager;
import ai.lzy.channelmanager.lock.GrainedLock;
import ai.lzy.channelmanager.model.Endpoint;
import ai.lzy.channelmanager.model.channel.Channel;
import ai.lzy.channelmanager.operation.ChannelOperationExecutor;
import ai.lzy.channelmanager.operation.state.DestroyActionState;
import ai.lzy.channelmanager.test.InjectedFailures;
import ai.lzy.longrunning.dao.OperationCompletedException;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.NotFoundException;
import ai.lzy.v1.channel.LCMPS;
import ai.lzy.v1.workflow.LzyWorkflowPrivateServiceGrpc;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Any;
import io.grpc.Status;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;

import static ai.lzy.model.db.DbHelper.withRetries;

public class DestroyAction extends ChannelAction {

    private static final Logger LOG = LogManager.getLogger(DestroyAction.class);

    private final DestroyActionState state;

    public DestroyAction(String operationId, Instant deadline, DestroyActionState state,
                         ObjectMapper objectMapper, ChannelOperationExecutor executor,
                         ChannelManagerDataSource storage, ChannelDao channelDao, OperationDao operationDao,
                         ChannelOperationDao channelOperationDao, ChannelController channelController,
                         SlotConnectionManager slotConnectionManager, GrainedLock lockManager,
                         LzyWorkflowPrivateServiceGrpc.LzyWorkflowPrivateServiceBlockingStub workflowPrivateApi)
    {
        super(operationId, deadline, objectMapper, executor, storage, channelDao, operationDao, channelOperationDao,
            channelController, slotConnectionManager, lockManager, workflowPrivateApi);
        this.state = state;
    }

    @Override
    public void run() {
        if (deadline.isBefore(Instant.now())) {
            LOG.info("Async operation (operationId={}) stopped, deadline exceeded", operationId);
            this.failOperation(state.executionId(), Status.DEADLINE_EXCEEDED);
            return;
        }

        LOG.info("Async operation (operationId={}) resumed. channelsToDestroy:{} {}, already destroyed:{} {}",
            operationId, state.toDestroyChannels().size(), state.toDestroyChannels(),
            state.destroyedChannels().size(), state.destroyedChannels());
        operationStopped = false;

        while (true) {
            String channelId = state.toDestroyChannels().stream().findFirst().orElse(null);

            if (channelId == null) {
                try {
                    withRetries(LOG, () -> {
                        try (var tx = TransactionHandle.create(storage)) {
                            channelOperationDao.delete(operationId, tx);
                            operationDao.complete(operationId,
                                Any.pack(LCMPS.ChannelDestroyResponse.getDefaultInstance()), tx);
                            tx.commit();
                        }
                    });
                    LOG.info("Async operation (operationId={}) finished", operationId);
                } catch (OperationCompletedException e) {
                    LOG.error("Async operation (operationId={}): failed to finish: already finished.", operationId);
                } catch (NotFoundException e) {
                    LOG.error("Async operation (operationId={}): failed to finish: not found.", operationId);
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

                InjectedFailures.fail9();

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

                InjectedFailures.fail10();

            } catch (InjectedFailures.InjectedException e) {
                throw e;
            } catch (Exception e) {
                String errorMessage = "Async operation (operationId=" + operationId + ") failed: " + e.getMessage();
                LOG.error(errorMessage);
                this.failOperation(state.executionId(), Status.INTERNAL.withDescription(errorMessage));
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

        for (Endpoint receiver : channel.getReceivers().asList()) {
            this.unbindReceiver(receiver, true);
        }

        for (Endpoint sender : channel.getSenders().asList()) {
            this.unbindSender(sender, true);
        }
    }

}
