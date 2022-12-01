package ai.lzy.channelmanager.v2.operation.action;

import ai.lzy.channelmanager.v2.control.ChannelController;
import ai.lzy.channelmanager.v2.db.ChannelDao;
import ai.lzy.channelmanager.v2.exceptions.CancellingChannelGraphStateException;
import ai.lzy.channelmanager.v2.operation.ChannelOperationDao;
import ai.lzy.channelmanager.v2.operation.state.UnbindActionState;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.v1.channel.v2.LCMS;
import com.google.protobuf.Any;
import io.grpc.Status;

import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.util.grpc.ProtoConverter.toProto;

public class UnbindAction implements ChannelAction {

    private final UnbindActionState state;
    private final ChannelDao channelDao;
    private final OperationDao operationDao;
    private final ChannelOperationDao channelOperationDao;
    private final ChannelController channelController;

    public UnbindAction(UnbindActionState state, ChannelDao channelDao, OperationDao operationDao,
                        ChannelOperationDao channelOperationDao, ChannelController channelController)
    {
        this.state = state;
        this.channelDao = channelDao;
        this.operationDao = operationDao;
        this.channelOperationDao = channelOperationDao;
        this.channelController = channelController;
    }

    @Override
    public void run() {
        try {
            LOG.info(operationDescription + " responded, async operation started, operationId={}", operation.id());

            switch (endpoint.getSlotDirection()) {
                case OUTPUT /* SENDER */ -> channelController.unbindSender(endpoint);
                case INPUT /* RECEIVER */ -> channelController.unbindReceiver(endpoint);
            }

            try {
                withRetries(LOG, () -> operationDao.updateResponse(operation.id(),
                    Any.pack(LCMS.BindResponse.getDefaultInstance()).toByteArray(), null));
            } catch (Exception e) {
                LOG.error("Cannot update operation", e);
                return;
            }

            LOG.info(operationDescription + " responded, async operation finished, operationId={}", operation.id());
        } catch (CancellingChannelGraphStateException e) {
            String errorMessage = operationDescription + " async operation " + operation.id()
                                  + " cancelled according to the graph state: " + e.getMessage();
            LOG.error(errorMessage);
            operationDao.failOperation(operation.id(),
                toProto(Status.CANCELLED.withDescription(errorMessage)), LOG);
        } catch (Exception e) {
            String errorMessage = operationDescription + " async operation " + operation.id()
                                  + " failed: " + e.getMessage();
            LOG.error(errorMessage);
            operationDao.failOperation(operation.id(),
                toProto(Status.INTERNAL.withDescription(errorMessage)), LOG);
            // TODO
        }

    }
}
