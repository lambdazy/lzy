package ai.lzy.channelmanager.v2.control;

import ai.lzy.channelmanager.v2.model.Endpoint;
import ai.lzy.longrunning.Operation;

public interface ChannelController {

    void executeBind(Endpoint endpoint, Operation bindOperation);

    void executeUnbind(Endpoint endpoint, Operation unbindOperation);

}
