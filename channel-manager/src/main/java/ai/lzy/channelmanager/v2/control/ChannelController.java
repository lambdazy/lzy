package ai.lzy.channelmanager.v2.control;

import ai.lzy.channelmanager.v2.exceptions.CancellingChannelGraphStateException;
import ai.lzy.channelmanager.v2.exceptions.IllegalChannelGraphStateException;
import ai.lzy.channelmanager.v2.model.Connection;
import ai.lzy.channelmanager.v2.model.Endpoint;

import javax.annotation.Nullable;

public interface ChannelController {

    @Nullable
    Endpoint findEndpointToConnect(Endpoint bindingEndpoint) throws CancellingChannelGraphStateException;
    boolean checkForSavingConnection(Endpoint bindingEndpoint, Endpoint connectedEndpoint)
        throws CancellingChannelGraphStateException;

    @Nullable
    Connection findConnectionToBreak(Endpoint unbindingReceiver) throws IllegalChannelGraphStateException;
    boolean checkForRemovingConnection(Endpoint unbindingReceiver, Endpoint connectedSender)
        throws IllegalChannelGraphStateException;

    @Nullable
    Endpoint findReceiverToUnbind(Endpoint unbindingSender) throws IllegalChannelGraphStateException;

}
