package ai.lzy.channelmanager.v2.operation.action;

import ai.lzy.channelmanager.db.ChannelManagerDataSource;
import ai.lzy.channelmanager.lock.GrainedLock;
import ai.lzy.channelmanager.v2.control.ChannelController;
import ai.lzy.channelmanager.v2.db.ChannelDao;
import ai.lzy.channelmanager.v2.model.Endpoint;
import ai.lzy.channelmanager.v2.operation.ChannelOperationDao;
import ai.lzy.channelmanager.v2.operation.state.UnbindActionState;
import ai.lzy.channelmanager.v2.slot.SlotApiClient;
import ai.lzy.channelmanager.v2.slot.SlotConnectionManager;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.v1.channel.v2.LCMS;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Any;
import io.grpc.Status;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static ai.lzy.model.db.DbHelper.withRetries;

public class UnbindAction extends ChannelAction {

    private static final Logger LOG = LogManager.getLogger(UnbindAction.class);

    private final UnbindActionState state;

    protected UnbindAction(ObjectMapper objectMapper, String operationId, ChannelManagerDataSource storage,
                           ChannelDao channelDao, OperationDao operationDao, ChannelOperationDao channelOperationDao,
                           SlotApiClient slotApiClient, ChannelController channelController, GrainedLock lockManager,
                           SlotConnectionManager slotConnectionManager, UnbindActionState state)
    {
        super(objectMapper, operationId, storage, channelDao, operationDao, channelOperationDao, slotApiClient,
            channelController, slotConnectionManager, lockManager);
        this.state = state;
    }

    @Override
    public void run() {
        LOG.info("Async operation (operationId={}) resumed, channelId={}", operationId, state.channelId());

        try {
            final Endpoint unbindingEndpoint = withRetries(LOG, () -> channelDao.findEndpoint(state.endpointUri(), null));

            if (unbindingEndpoint == null) {
                try {
                    withRetries(LOG, () -> {
                        try (var tx = TransactionHandle.create(storage)) {
                            channelOperationDao.delete(operationId, tx);
                            operationDao.updateResponse(operationId,
                                Any.pack(LCMS.UnbindResponse.getDefaultInstance()).toByteArray(), tx);
                            tx.commit();
                        }
                    });
                    LOG.info("Async operation (operationId={}) force finished, endpoint {} of channel {} not found",
                        operationId, state.endpointUri(), state.channelId());
                } catch (Exception e) {
                    LOG.error("Async operation (operationId={}): failed to finish: {}. Restart action",
                        operationId, e.getMessage());
                    scheduleRestart();
                }
                return;
            }


            switch (unbindingEndpoint.getSlotDirection()) {
                case OUTPUT /* SENDER */ -> this.unbindSender(unbindingEndpoint);
                case INPUT /* RECEIVER */ -> this.unbindReceiver(unbindingEndpoint);
            }

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
            } catch (Exception e) {
                LOG.error("Async operation (operationId={}): failed to finish: {}. Restart action",
                    operationId, e.getMessage());
                scheduleRestart();
            }

        } catch (Exception e) {
            String errorMessage = "Async operation (operationId=" + operationId + ") failed: " + e.getMessage();
            LOG.error(errorMessage);
            this.failOperation(Status.INTERNAL.withDescription(errorMessage));
        }
    }

}
