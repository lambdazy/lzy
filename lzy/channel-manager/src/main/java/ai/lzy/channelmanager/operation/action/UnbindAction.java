package ai.lzy.channelmanager.operation.action;

import ai.lzy.channelmanager.control.ChannelController;
import ai.lzy.channelmanager.dao.ChannelDao;
import ai.lzy.channelmanager.dao.ChannelManagerDataSource;
import ai.lzy.channelmanager.dao.ChannelOperationDao;
import ai.lzy.channelmanager.grpc.SlotConnectionManager;
import ai.lzy.channelmanager.lock.GrainedLock;
import ai.lzy.channelmanager.model.Endpoint;
import ai.lzy.channelmanager.operation.ChannelOperationExecutor;
import ai.lzy.channelmanager.operation.state.UnbindActionState;
import ai.lzy.channelmanager.test.InjectedFailures;
import ai.lzy.longrunning.dao.OperationCompletedException;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.NotFoundException;
import ai.lzy.v1.channel.LCMS;
import ai.lzy.v1.workflow.LzyWorkflowPrivateServiceGrpc;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Any;
import io.grpc.Status;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;
import java.time.Instant;

import static ai.lzy.model.db.DbHelper.withRetries;

public class UnbindAction extends ChannelAction {

    private static final Logger LOG = LogManager.getLogger(UnbindAction.class);

    private final UnbindActionState state;

    public UnbindAction(String operationId, Instant deadline, UnbindActionState state,
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
            LOG.info("Async operation (operationId={}) stopped, deadline exceeded, channelId={}",
                operationId, state.channelId());
            this.failOperation(state.wfName(), state.executionId(), Status.DEADLINE_EXCEEDED);
            return;
        }

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
                case OUTPUT /* SENDER */ -> this.unbindSender(unbindingEndpoint, false);
                case INPUT /* RECEIVER */ -> this.unbindReceiver(unbindingEndpoint, false);
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
            this.failOperation(state.wfName(), state.executionId(), Status.INTERNAL.withDescription(errorMessage));
        }
    }

    private void finishOperation() throws Exception {
        try {
            withRetries(LOG, () -> {
                try (var tx = TransactionHandle.create(storage)) {
                    channelOperationDao.delete(operationId, tx);
                    operationDao.complete(operationId, Any.pack(LCMS.UnbindResponse.getDefaultInstance()), tx);
                    tx.commit();
                }
            });
            LOG.info("Async operation (operationId={}) finished", operationId);
            operationStopped = true;
        } catch (OperationCompletedException e) {
            LOG.error("Async operation (operationId={}): failed to finish: already finished.", operationId);
        } catch (NotFoundException e) {
            LOG.error("Async operation (operationId={}): failed to finish: not found.", operationId);
        } catch (SQLException e) {
            LOG.error("Async operation (operationId={}): failed to finish: {}. Schedule restart action",
                operationId, e.getMessage());
            scheduleRestart();
        }
    }

}
