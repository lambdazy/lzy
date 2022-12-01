package ai.lzy.channelmanager.v2.operation.action;

import ai.lzy.channelmanager.v2.control.ChannelController;
import ai.lzy.channelmanager.v2.db.ChannelDao;
import ai.lzy.channelmanager.v2.operation.ChannelOperationDao;
import ai.lzy.channelmanager.v2.operation.state.DestroyActionState;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.v1.channel.v2.LCMPS;
import com.google.protobuf.Any;

import static ai.lzy.model.db.DbHelper.withRetries;

public class DestroyAction implements ChannelAction {

    private final String operationId;
    private final DestroyActionState state;
    private final ChannelDao channelDao;
    private final OperationDao operationDao;
    private final ChannelOperationDao channelOperationDao;
    private final ChannelController channelController;

    public DestroyAction(DestroyActionState state, ChannelDao channelDao, OperationDao operationDao,
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
        String channelId = state.toDestroyChannels().stream().findFirst().orElse(null);
        if (channelId == null) {
            withRetries(LOG, () -> operationDao.updateResponse(operationId,
                Any.pack(LCMPS.ChannelDestroyResponse.getDefaultInstance()).toByteArray(), null));
            // update op state in dao, set op done
            LOG.info(operationDescription + " async operation (operationId={}) finished", operation.id());
            return;
        }
        LOG.info(operationDescription + " async operation (operationId={}) resumed, channelId={}", channelId);

        try {
            channelController.destroy(channelId);

            state.setDestroyed(channelId);


            // update chop state in dao
            try {

            } catch (Exception e) {
                LOG.error("Cannot update operation", e);
                return;
            }

        } catch (Exception e) {
            String errorMessage = operationDescription + " async operation (operationId=" + operation.id()
                                  + ") failed: " + e.getMessage();
            LOG.error(errorMessage);
            failOperation(operation.id(), errorMessage);
            // TODO
        }

        restart();
    }
}
