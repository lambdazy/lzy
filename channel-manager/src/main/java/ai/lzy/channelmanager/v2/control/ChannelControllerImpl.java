package ai.lzy.channelmanager.v2.control;

import ai.lzy.channelmanager.lock.GrainedLock;
import ai.lzy.channelmanager.v2.db.ChannelDao;
import ai.lzy.channelmanager.v2.exceptions.CancellingChannelGraphStateException;
import ai.lzy.channelmanager.v2.exceptions.IllegalChannelGraphStateException;
import ai.lzy.channelmanager.v2.model.Channel;
import ai.lzy.channelmanager.v2.model.Connection;
import ai.lzy.channelmanager.v2.model.Endpoint;
import ai.lzy.channelmanager.v2.slot.SlotApiClient;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import javax.annotation.Nullable;

import static ai.lzy.model.db.DbHelper.withRetries;

@Singleton
public class ChannelControllerImpl implements ChannelController {

    private static final Logger LOG = LogManager.getLogger(ChannelControllerImpl.class);

    private final ChannelDao channelDao;
    private final GrainedLock lockManager;
    private final SlotApiClient slotApiClient;

    public ChannelControllerImpl(ChannelDao channelDao, GrainedLock lockManager, SlotApiClient slotApiClient) {
        this.channelDao = channelDao;
        this.lockManager = lockManager;
        this.slotApiClient = slotApiClient;
    }

    @Override
    @Nullable
    public Endpoint findEndpointToConnect(Endpoint bindingEndpoint) throws CancellingChannelGraphStateException {
        LOG.debug("[findEndpointToConnect], bindingEndpoint={}", bindingEndpoint.getUri());

        final String channelId = bindingEndpoint.getChannelId();
        final Channel channel;
        try {
            channel = withRetries(LOG, () -> channelDao.findChannel(channelId, Channel.LifeStatus.ALIVE, null));
        } catch (Exception e) {
            throw new RuntimeException("Failed to find channel in storage", e);
        }
        if (channel == null) {
            throw new CancellingChannelGraphStateException(channelId, "Channel not found");
        }

        final Endpoint actualStateEndpoint = channel.getEndpoint(bindingEndpoint.getUri());
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
                channel.findReceiversToConnect(bindingEndpoint).stream().findFirst().orElse(null);
            case INPUT /*RECEIVER */ ->
                channel.findSenderToConnect(bindingEndpoint);
        };

        if (endpointToConnect == null) {
            LOG.debug("[findEndpointToConnect] done, nothing to connect, bindingEndpoint={}", bindingEndpoint.getUri());
            return null;
        }

        LOG.debug("[findEndpointToConnect] done, bindingEndpoint={}, found endpointToConnect={}",
            bindingEndpoint.getUri(), endpointToConnect.getUri());

        return endpointToConnect;
    }

    @Override
    public boolean checkForSavingConnection(Endpoint bindingEndpoint, Endpoint connectedEndpoint)
        throws CancellingChannelGraphStateException
    {
        LOG.debug("[checkForSavingConnection], bindingEndpoint={}, connectedEndpoint={}",
            bindingEndpoint.getUri(), connectedEndpoint.getUri());

        final String channelId = bindingEndpoint.getChannelId();
        final Endpoint sender;
        final Endpoint receiver;
        switch (bindingEndpoint.getSlotDirection()) {
            case OUTPUT /* SENDER */ -> {
                sender = bindingEndpoint;
                receiver = connectedEndpoint;
            }
            case INPUT /* RECEIVER */ -> {
                sender = connectedEndpoint;
                receiver = bindingEndpoint;
            }
            default -> throw new IllegalStateException(
                "Endpoint " + bindingEndpoint.getUri()
                + " has unexpected direction" + bindingEndpoint.getSlotDirection());
        }

        final Channel channel;
        try {
            channel = withRetries(LOG, () -> channelDao.findChannel(channelId, Channel.LifeStatus.ALIVE, null));
        } catch (Exception e) {
            throw new RuntimeException("Failed to find channel in storage", e);
        }
        if (channel == null) {
            throw new CancellingChannelGraphStateException(channelId, "Channel not found");
        }

        final Endpoint actualStateEndpoint = channel.getEndpoint(bindingEndpoint.getUri());
        if (actualStateEndpoint == null) {
            throw new CancellingChannelGraphStateException(channelId,
                "Endpoint " + bindingEndpoint.getUri() + " not found");
        }

        if (actualStateEndpoint.getStatus() != Endpoint.LifeStatus.BINDING) {
            throw new CancellingChannelGraphStateException(channelId,
                "Endpoint " + bindingEndpoint.getUri() + " has wrong lifeStatus " + actualStateEndpoint.getStatus());
        }

        final Connection actualStateConnection = channel.getConnection(sender.getUri(), receiver.getUri());
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
    public Endpoint findReceiverToUnbind(Endpoint unbindingSender) throws IllegalChannelGraphStateException {
        LOG.debug("[findReceiverToUnbind], unbindingSender={}", unbindingSender.getUri());

        final String channelId = unbindingSender.getChannelId();
        final Channel channel;
        try {
            channel = withRetries(LOG, () -> channelDao.findChannel(channelId, null));
        } catch (Exception e) {
            throw new RuntimeException("Failed to find channel in storage", e);
        }
        if (channel == null) {
            LOG.warn("[findReceiverToUnbind] skipped, channel already removed, unbindingSender={}",
                unbindingSender.getUri());
            return null;
        }

        final Endpoint actualStateEndpoint = channel.getEndpoint(unbindingSender.getUri());
        if (actualStateEndpoint == null) {
            LOG.warn("[findReceiverToUnbind] skipped, sender already removed, unbindingSender={}",
                unbindingSender.getUri());
            return null;
        }

        if (actualStateEndpoint.getStatus() != Endpoint.LifeStatus.UNBINDING) {
            throw new IllegalChannelGraphStateException(channelId,
                "Endpoint " + unbindingSender.getUri() + " has wrong lifeStatus " + actualStateEndpoint.getStatus());
        }

        final List<Connection> actualStateConnections = channel.getConnectionsOfEndpoint(unbindingSender.getUri());
        final Endpoint receiverToUnbind = actualStateConnections.stream()
            .filter(conn -> conn.status() == Connection.LifeStatus.CONNECTED)
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
    public Connection findConnectionToBreak(Endpoint unbindingReceiver) throws IllegalChannelGraphStateException {
        LOG.debug("[findConnectionToBreak], unbindingReceiver={}", unbindingReceiver.getUri());

        final String channelId = unbindingReceiver.getChannelId();
        final Channel channel;
        try {
            channel = withRetries(LOG, () -> channelDao.findChannel(channelId, null));
        } catch (Exception e) {
            throw new RuntimeException("Failed to find channel in storage", e);
        }
        if (channel == null) {
            LOG.warn("[findConnectionToBreak] skipped, channel already removed, unbindingReceiver={}",
                unbindingReceiver.getUri());
            return null;
        }

        final Endpoint actualStateEndpoint = channel.getEndpoint(unbindingReceiver.getUri());
        if (actualStateEndpoint == null) {
            LOG.warn("[findConnectionToBreak] skipped, receiver already removed, unbindingReceiver={}",
                unbindingReceiver.getUri());
            return null;
        }

        if (actualStateEndpoint.getStatus() != Endpoint.LifeStatus.UNBINDING) {
            throw new IllegalChannelGraphStateException(channelId,
                "Endpoint " + unbindingReceiver.getUri() + " has wrong lifeStatus " + actualStateEndpoint.getStatus());
        }

        final List<Connection> actualStateConnections = channel.getConnectionsOfEndpoint(unbindingReceiver.getUri());
        final Connection connectionToBreak = actualStateConnections.stream()
            .filter(it -> it.status() == Connection.LifeStatus.DISCONNECTING)
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
