package ai.lzy.channelmanager.v2.operation.action;

import ai.lzy.channelmanager.db.ChannelManagerDataSource;
import ai.lzy.channelmanager.lock.GrainedLock;
import ai.lzy.channelmanager.v2.control.ChannelController;
import ai.lzy.channelmanager.v2.dao.ChannelDao;
import ai.lzy.channelmanager.v2.dao.ChannelOperationDao;
import ai.lzy.channelmanager.v2.debug.InjectedFailures;
import ai.lzy.channelmanager.v2.grpc.SlotConnectionManager;
import ai.lzy.channelmanager.v2.model.Endpoint;
import ai.lzy.channelmanager.v2.operation.ChannelOperationExecutor;
import ai.lzy.channelmanager.v2.operation.state.UnbindActionState;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.v1.channel.v2.LCMS;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Any;
import io.grpc.Status;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;

import static ai.lzy.model.db.DbHelper.withRetries;

public class UnbindAction extends ChannelAction {

    private static final Logger LOG = LogManager.getLogger(UnbindAction.class);

    private final UnbindActionState state;

    public UnbindAction(String operationId, UnbindActionState state,
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
        LOG.info("Async operation (operationId={}) resumed, channelId={}", operationId, state.channelId());
        operationStopped = false;

        try {
            final Endpoint unbindingEndpoint =
                withRetries(LOG, () -> channelDao.findEndpoint(state.endpointUri(), null));

            if (unbindingEndpoint == null) {
                finishOperation();
                return;
            }

            switch (unbindingEndpoint.getSlotDirection()) {
                case OUTPUT /* SENDER */ -> this.unbindSender(unbindingEndpoint);
                case INPUT /* RECEIVER */ -> this.unbindReceiver(unbindingEndpoint);
            }
            if (operationStopped) {
                return;
            }

            finishOperation();

        } catch (InjectedFailures.InjectedException e) {
            throw e;
        } catch (Exception e) {
            String errorMessage = "Async operation (operationId=" + operationId + ") failed: " + e.getMessage();
            LOG.error(errorMessage);
            this.failOperation(Status.INTERNAL.withDescription(errorMessage));
        }
    }

    private void finishOperation() throws Exception {
        try {
            withRetries(LOG, () -> {
                try (var tx = TransactionHandle.create(storage)) {
                    channelOperationDao.delete(operationId, tx);
                    operationDao.updateResponse(operationId,
                        Any.pack(LCMS.UnbindResponse.getDefaultInstance()).toByteArray(), tx);
                    tx.commit();
                }
            });
            LOG.info("Async operation (operationId={}) finished", operationId);
            operationStopped = true;
        } catch (SQLException e) {
            LOG.error("Async operation (operationId={}): failed to finish: {}. Schedule restart action",
                operationId, e.getMessage());
            scheduleRestart();
        }
    }

}
