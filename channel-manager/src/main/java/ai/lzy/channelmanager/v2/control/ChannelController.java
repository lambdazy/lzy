package ai.lzy.channelmanager.v2.control;

import ai.lzy.channelmanager.v2.model.Endpoint;

public interface ChannelController {

    void executeBind(Endpoint endpoint);

    void executeUnbind(Endpoint endpoint);

}
