package ai.lzy.channelmanager.control;

import ai.lzy.channelmanager.exceptions.CancellingChannelGraphStateException;
import ai.lzy.channelmanager.exceptions.IllegalChannelGraphStateException;
import ai.lzy.channelmanager.model.Connection;
import ai.lzy.channelmanager.model.Endpoint;
import ai.lzy.channelmanager.model.channel.Channel;
import jakarta.annotation.Nullable;

// TODO (lindvv): move to Channel.java
public interface ChannelController {

    @Nullable
    Endpoint findEndpointToConnect(Channel actualChannel, Endpoint bindingEndpoint)
        throws CancellingChannelGraphStateException;

    boolean checkChannelForSavingConnection(Channel actualChannel, Endpoint bindingEndpoint, Endpoint connectedEndpoint)
        throws CancellingChannelGraphStateException;

    @Nullable
    Endpoint findReceiverToUnbind(Channel actualChannel, Endpoint unbindingSender)
        throws IllegalChannelGraphStateException;

    @Nullable
    Connection findConnectionToBreak(Channel actualChannel, Endpoint unbindingReceiver)
        throws IllegalChannelGraphStateException;

}
