package ai.lzy.channelmanager.control;

import ai.lzy.channelmanager.exceptions.CancellingChannelGraphStateException;
import ai.lzy.channelmanager.exceptions.IllegalChannelGraphStateException;
import ai.lzy.channelmanager.model.Connection;
import ai.lzy.channelmanager.model.Endpoint;
import ai.lzy.channelmanager.model.channel.Channel;
import jakarta.annotation.Nullable;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@Singleton
public class ChannelControllerImpl implements ChannelController {

    private static final Logger LOG = LogManager.getLogger(ChannelControllerImpl.class);

    @Override
    @Nullable
    public Endpoint findEndpointToConnect(Channel actualChannel, Endpoint bindingEndpoint)
        throws CancellingChannelGraphStateException
    {
        LOG.debug("[findEndpointToConnect], bindingEndpoint={}", bindingEndpoint.getUri());

        String channelId = actualChannel.getId();

        if (actualChannel.getLifeStatus() != Channel.LifeStatus.ALIVE) {
            throw new CancellingChannelGraphStateException(channelId,
                "Channel " + channelId + " has wrong lifeStatus " + actualChannel.getLifeStatus());
        }

        final Endpoint actualStateEndpoint = actualChannel.getEndpoint(bindingEndpoint.getUri());
        if (actualStateEndpoint == null) {
            throw new CancellingChannelGraphStateException(channelId,
                "Endpoint " + bindingEndpoint.getUri() + " not found");
        }

        if (actualStateEndpoint.getStatus() != Endpoint.LifeStatus.BINDING) {
            throw new CancellingChannelGraphStateException(channelId,
                "Endpoint " + bindingEndpoint.getUri() + " has wrong lifeStatus " + actualStateEndpoint.getStatus());
        }

        final Endpoint endpointToConnect = switch (bindingEndpoint.getSlotDirection()) {
            case OUTPUT /* SENDER */ ->
                actualChannel.findReceiversToConnect(bindingEndpoint).stream().findFirst().orElse(null);
            case INPUT /*RECEIVER */ ->
                actualChannel.findSenderToConnect(bindingEndpoint);
        };

        if (endpointToConnect == null) {
            LOG.debug("[findEndpointToConnect] done, nothing to connect, bindingEndpoint={}", bindingEndpoint.getUri());
            return null;
        }

        LOG.debug("[findEndpointToConnect] done, found endpointToConnect={}, bindingEndpoint={}",
            endpointToConnect.getUri(), bindingEndpoint.getUri());

        return endpointToConnect;
    }

    @Override
    public boolean checkChannelForSavingConnection(Channel actualChannel, Endpoint bindingEndpoint,
                                                   Endpoint connectedEndpoint)
        throws CancellingChannelGraphStateException
    {
        LOG.debug("[checkForSavingConnection], bindingEndpoint={}, connectedEndpoint={}",
            bindingEndpoint.getUri(), connectedEndpoint.getUri());

        String channelId = actualChannel.getId();
        Connection connection = Connection.of(bindingEndpoint, connectedEndpoint);
        final Endpoint sender = connection.sender();
        final Endpoint receiver = connection.receiver();

        if (actualChannel.getLifeStatus() != Channel.LifeStatus.ALIVE) {
            throw new CancellingChannelGraphStateException(channelId,
                "Channel " + channelId + " has wrong lifeStatus " + actualChannel.getLifeStatus());
        }
        final Endpoint actualStateEndpoint = actualChannel.getEndpoint(bindingEndpoint.getUri());
        if (actualStateEndpoint == null) {
            throw new CancellingChannelGraphStateException(channelId,
                "Endpoint " + bindingEndpoint.getUri() + " not found");
        }

        if (actualStateEndpoint.getStatus() != Endpoint.LifeStatus.BINDING) {
            throw new CancellingChannelGraphStateException(channelId,
                "Endpoint " + bindingEndpoint.getUri() + " has wrong lifeStatus " + actualStateEndpoint.getStatus());
        }

        final Connection actualStateConnection = actualChannel.getConnection(sender.getUri(), receiver.getUri());

        if (actualStateConnection == null || actualStateConnection.status() != Connection.LifeStatus.CONNECTING) {
            String connectionStatus = actualStateConnection == null ? "null" : actualStateConnection.status().name();
            LOG.warn("[checkForSavingConnection] failed, unexpected connection status {}, "
                     + "bindingEndpoint={}, connectedEndpoint={}",
                connectionStatus, bindingEndpoint.getUri(), connectedEndpoint.getUri());
            return false;
        }

        LOG.debug("[checkForSavingConnection] ok, bindingEndpoint={}, connectedEndpoint={}",
            bindingEndpoint.getUri(), connectedEndpoint.getUri());

        return true;
    }

    @Override
    @Nullable
    public Endpoint findReceiverToUnbind(Channel actualChannel, Endpoint unbindingSender)
        throws IllegalChannelGraphStateException
    {
        LOG.debug("[findReceiverToUnbind], unbindingSender={}", unbindingSender.getUri());

        final String channelId = unbindingSender.getChannelId();

        final Endpoint actualStateEndpoint = actualChannel.getEndpoint(unbindingSender.getUri());
        if (actualStateEndpoint == null) {
            LOG.warn("[findReceiverToUnbind] skipped, sender already removed, unbindingSender={}",
                unbindingSender.getUri());
            return null;
        }

        if (actualStateEndpoint.getStatus() != Endpoint.LifeStatus.UNBINDING) {
            throw new IllegalChannelGraphStateException(channelId,
                "Endpoint " + unbindingSender.getUri() + " has wrong lifeStatus " + actualStateEndpoint.getStatus());
        }

        final Endpoint receiverToUnbind = actualChannel.getConnectionsOfEndpoint(unbindingSender.getUri())
            .stream()
            .map(Connection::receiver)
            .findFirst().orElse(null);

        if (receiverToUnbind == null) {
            LOG.debug("[findReceiverToUnbind] done, nothing to unbind, unbindingSender={}", unbindingSender.getUri());
            return null;
        }

        LOG.debug("[findReceiverToUnbind] done, unbindingSender={}, foundReceiver={}",
            unbindingSender.getUri(), receiverToUnbind.getUri());

        return receiverToUnbind;
    }

    @Override
    @Nullable
    public Connection findConnectionToBreak(Channel actualChannel, Endpoint unbindingReceiver)
        throws IllegalChannelGraphStateException
    {
        LOG.debug("[findConnectionToBreak], unbindingReceiver={}", unbindingReceiver.getUri());

        String channelId = actualChannel.getId();

        final Endpoint actualStateEndpoint = actualChannel.getEndpoint(unbindingReceiver.getUri());
        if (actualStateEndpoint == null) {
            LOG.warn("[findConnectionToBreak] skipped, receiver already removed, unbindingReceiver={}",
                unbindingReceiver.getUri());
            return null;
        }

        if (actualStateEndpoint.getStatus() != Endpoint.LifeStatus.UNBINDING) {
            throw new IllegalChannelGraphStateException(channelId,
                "Endpoint " + unbindingReceiver.getUri() + " has wrong lifeStatus " + actualStateEndpoint.getStatus());
        }

        final Connection connectionToBreak = actualChannel.getConnectionsOfEndpoint(unbindingReceiver.getUri())
            .stream()
            .findFirst().orElse(null);

        if (connectionToBreak == null) {
            LOG.debug("[findConnectionToBreak] done, nothing to disconnect, unbindingReceiver={}",
                unbindingReceiver.getUri());
            return null;
        }

        LOG.debug("[findConnectionToBreak] done, connection found, unbindingReceiver={}, foundConnectedSender={}",
            unbindingReceiver.getUri(), connectionToBreak.sender().getUri());

        return connectionToBreak;
    }

}
