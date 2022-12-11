package ai.lzy.channelmanager.v2.control;

import ai.lzy.channelmanager.v2.exceptions.CancellingChannelGraphStateException;
import ai.lzy.channelmanager.v2.exceptions.IllegalChannelGraphStateException;
import ai.lzy.channelmanager.v2.model.Channel;
import ai.lzy.channelmanager.v2.model.Connection;
import ai.lzy.channelmanager.v2.model.Endpoint;

import javax.annotation.Nullable;

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
