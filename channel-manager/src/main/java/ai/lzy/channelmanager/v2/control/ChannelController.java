package ai.lzy.channelmanager.v2.control;

import ai.lzy.channelmanager.v2.exceptions.ChannelGraphStateException;
import ai.lzy.channelmanager.v2.model.Endpoint;

public interface ChannelController {

    void bind(Endpoint endpoint) throws ChannelGraphStateException;

    void unbindSender(Endpoint sender) throws ChannelGraphStateException;
    void unbindReceiver(Endpoint receiver) throws ChannelGraphStateException;

    void destroy(String channelId) throws ChannelGraphStateException;

}
