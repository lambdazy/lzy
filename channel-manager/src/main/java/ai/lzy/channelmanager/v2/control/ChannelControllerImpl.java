package ai.lzy.channelmanager.v2.control;

import ai.lzy.channelmanager.lock.GrainedLock;
import ai.lzy.channelmanager.v2.db.ChannelStorage;
import ai.lzy.channelmanager.v2.exceptions.CancellingChannelGraphStateException;
import ai.lzy.channelmanager.v2.exceptions.ChannelGraphStateException;
import ai.lzy.channelmanager.v2.exceptions.IllegalChannelGraphStateException;
import ai.lzy.channelmanager.v2.model.Channel;
import ai.lzy.channelmanager.v2.model.Connection;
import ai.lzy.channelmanager.v2.model.Endpoint;
import ai.lzy.channelmanager.v2.slot.SlotApiClient;
import ai.lzy.model.db.exceptions.NotFoundException;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.List;
import javax.annotation.Nullable;

import static ai.lzy.model.db.DbHelper.withRetries;

@Singleton
public class ChannelControllerImpl implements ChannelController {

    private static final Logger LOG = LogManager.getLogger(ChannelControllerImpl.class);

    private final ChannelStorage channelStorage;
    private final GrainedLock lockManager;
    private final SlotApiClient slotApiClient;

    public ChannelControllerImpl(ChannelStorage channelStorage, GrainedLock lockManager, SlotApiClient slotApiClient) {
        this.channelStorage = channelStorage;
        this.lockManager = lockManager;
        this.slotApiClient = slotApiClient;
    }

    @Override
    public void bind(Endpoint endpoint) throws ChannelGraphStateException {
        final String channelId = endpoint.getChannelId();
        while (true) {
            final Endpoint endpointToConnect;

            try (final var guard = lockManager.withLock(channelId)) {
                endpointToConnect = findEndpointToConnect(endpoint);
                if (endpointToConnect == null) {
                    try {
                        withRetries(LOG, () -> channelStorage.markEndpointBound(endpoint.getUri().toString(), null));
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to mark endpoint bound in storage");
                    }
                    return;
                }
            }

            Connection potentialConnection = Connection.of(endpoint, endpointToConnect);
            try {
                slotApiClient.connect(potentialConnection.sender(), potentialConnection.receiver(),
                    Duration.ofSeconds(10));
            } catch (Exception e) {
                LOG.debug("[bind] removing connection from storage after failed connection attempt,"
                          + " sender={}, receiver={}",
                    potentialConnection.sender().getUri(), potentialConnection.receiver().getUri());
                try (final var guard = lockManager.withLock(channelId)) {
                    withRetries(LOG, () -> channelStorage.removeEndpointConnection(
                        channelId,
                        potentialConnection.sender().getUri().toString(),
                        potentialConnection.receiver().getUri().toString(),
                        null));
                } catch (Exception ex) {
                    throw new RuntimeException("Failed to remove connection in storage");
                }
                throw e;
            }

            boolean connectionSaved;
            try (final var guard = lockManager.withLock(endpoint.getChannelId())) {
                connectionSaved = saveConnection(endpoint, endpointToConnect);
            } catch (CancellingChannelGraphStateException e) {
                LOG.info("[bind] disconnecting endpoints after CancellingException, sender={}, receiver={}",
                    potentialConnection.sender().getUri(), potentialConnection.receiver().getUri());
                slotApiClient.disconnect(potentialConnection.receiver());
                throw e;
            }

            if (!connectionSaved) {
                LOG.info("[bind] disconnecting endpoints after saving connection fail, sender={}, receiver={}",
                    potentialConnection.sender().getUri(), potentialConnection.receiver().getUri());
                slotApiClient.disconnect(potentialConnection.receiver());
            }
        }
    }

    @Override
    public void unbindSender(Endpoint sender) throws ChannelGraphStateException {
        final String channelId = sender.getChannelId();
        while (true) {
            final Endpoint receiverToUnbind;
            try (final var guard = lockManager.withLock(sender.getChannelId())) {
                receiverToUnbind = findReceiverToUnbind(sender);
                if (receiverToUnbind == null) {
                    break;
                }
            }

            LOG.warn("[unbindSender], found bound receiver, need force unbind, sender={}, receiver={}",
                sender.getUri(), receiverToUnbind.getUri());
            unbindReceiver(receiverToUnbind);
        }

        slotApiClient.disconnect(sender);

        // TODO (lindvv): maybe should suspend, so shouldn't destroy, fix after slots refactoring
        slotApiClient.destroy(sender);
        sender.invalidate();

        try (final var guard = lockManager.withLock(channelId)) {
            withRetries(LOG, () -> channelStorage.removeEndpointWithoutConnections(sender.getUri().toString(), null));
        } catch (NotFoundException e) {
            LOG.info("[unbindSender] removing endpoint skipped, sender already removed, sender={}", sender.getUri());
        } catch (Exception e) {
            throw new RuntimeException("Failed to remove endpoint in storage");
        }
    }

    @Override
    public void unbindReceiver(Endpoint receiver) throws ChannelGraphStateException {
        final String channelId = receiver.getChannelId();

        final Connection connectionToBreak;
        try (final var guard = lockManager.withLock(channelId)) {
            connectionToBreak = findConnectionToBreak(receiver);
            if (connectionToBreak == null) {
                try {
                    withRetries(LOG, () ->
                        channelStorage.removeEndpointWithoutConnections(receiver.getUri().toString(), null));
                } catch (Exception e) {
                    throw new RuntimeException("Failed to remove endpoint in storage");
                }
                return;
            }
        }

        slotApiClient.disconnect(receiver);

        // TODO (lindvv): maybe should suspend, so shouldn't destroy, fix after slots refactoring
        slotApiClient.destroy(receiver);
        receiver.invalidate();

        try (final var guard = lockManager.withLock(channelId)) {
            boolean connectionRemoved = removeConnection(connectionToBreak.receiver(), connectionToBreak.sender());
            if (connectionRemoved) {
                try {
                    withRetries(LOG, () ->
                        channelStorage.removeEndpointWithoutConnections(receiver.getUri().toString(), null));
                } catch (Exception e) {
                    throw new RuntimeException("Failed to remove endpoint in storage");
                }
            }
        }

    }

    @Override
    public void destroy(String channelId) throws ChannelGraphStateException {
        final Channel channel;
        try (final var guard = lockManager.withLock(channelId)) {
            try {
                channel = withRetries(LOG, () ->
                    channelStorage.findChannel(channelId, Channel.LifeStatus.DESTROYING, null));
            } catch (Exception e) {
                throw new RuntimeException("Failed to find destroying channel in storage", e);
            }
            if (channel == null) {
                LOG.info("[destroy] skipped, channel {} already removed", channelId);
                return;
            }
            try {
                withRetries(LOG, () -> channelStorage.markAllEndpointsUnbinding(channelId, null));
            } catch (Exception e) {
                throw new RuntimeException("Failed to mark channel endpoints unbinding in storage", e);
            }
        }

        for (Endpoint receiver : channel.getActiveReceivers().asList()) {
            unbindReceiver(receiver);
        }

        for (Endpoint sender : channel.getActiveSenders().asList()) {
            unbindSender(sender);
        }

        try (final var guard = lockManager.withLock(channelId)) {
            withRetries(LOG, () -> channelStorage.removeChannel(channelId, null));
        } catch (NotFoundException e) {
            LOG.info("[destroy] removing channel skipped, channel {} already removed", channelId);
        } catch (Exception e) {
            throw new RuntimeException("Failed to remove channel in storage", e);
        }
    }

    @Nullable
    private Endpoint findEndpointToConnect(Endpoint bindingEndpoint) throws CancellingChannelGraphStateException {
        LOG.debug("[findEndpointToConnect], bindingEndpoint={}", bindingEndpoint.getUri());

        final String channelId = bindingEndpoint.getChannelId();
        final Channel channel;
        try {
            channel = withRetries(LOG, () -> channelStorage.findChannel(channelId, Channel.LifeStatus.ALIVE, null));
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

        final Endpoint endpointToConnect;
        final Endpoint sender;
        final Endpoint receiver;
        switch (bindingEndpoint.getSlotDirection()) {
            case OUTPUT /* SENDER */ -> {
                endpointToConnect = channel.findSenderToConnect(bindingEndpoint);
                sender = bindingEndpoint;
                receiver = endpointToConnect;
            }
            case INPUT /* RECEIVER */ -> {
                endpointToConnect = channel.findReceiversToConnect(bindingEndpoint).stream().findFirst().orElse(null);
                sender = endpointToConnect;
                receiver = bindingEndpoint;
            }
            default -> throw new IllegalStateException(
                "Endpoint " + bindingEndpoint.getUri() + " has unexpected direction");
        }

        if (endpointToConnect == null) {
            LOG.debug("[findEndpointToConnect] done, nothing to connect, bindingEndpoint={}", bindingEndpoint.getUri());
            return null;
        }

        try {
            withRetries(LOG, () -> channelStorage.insertConnection(channelId, Connection.of(sender, receiver), null));
        } catch (Exception e) {
            throw new RuntimeException("Failed to insert connection in storage", e);
        }

        LOG.debug("[findEndpointToConnect] done, bindingEndpoint={}, found endpointToConnect={}",
            bindingEndpoint.getUri(), endpointToConnect.getUri());

        return endpointToConnect;
    }

    private boolean saveConnection(Endpoint bindingEndpoint, Endpoint connectedEndpoint)
        throws CancellingChannelGraphStateException
    {
        LOG.debug("[saveConnection], bindingEndpoint={}, connectedEndpoint={}",
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
                "Endpoint " + bindingEndpoint.getUri() + " has unexpected direction" + bindingEndpoint.getSlotDirection());
        }

        final Channel channel;
        try {
            channel = withRetries(LOG, () -> channelStorage.findChannel(channelId, Channel.LifeStatus.ALIVE, null));
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
            LOG.warn("[saveConnection] skipped, unexpected connection status {}, "
                      + "bindingEndpoint={}, connectedEndpoint={}",
                connectionStatus, bindingEndpoint.getUri(), connectedEndpoint.getUri());
            return false;
        }

        try {
            withRetries(LOG, () -> channelStorage.markConnectionAlive(
                channelId, sender.getUri().toString(), receiver.getUri().toString(), null));
        } catch (Exception e) {
            throw new RuntimeException("Failed to save alive connection in storage", e);
        }

        LOG.debug("[saveConnection] done, bindingEndpoint={}, connectedEndpoint={}",
            bindingEndpoint.getUri().toString(), connectedEndpoint.getUri().toString());

        return true;
    }

    @Nullable
    private Endpoint findReceiverToUnbind(Endpoint unbindingSender) throws IllegalChannelGraphStateException {
        LOG.debug("[findReceiverToUnbind], unbindingSender={}", unbindingSender.getUri());

        final String channelId = unbindingSender.getChannelId();
        final Channel channel;
        try {
            channel = withRetries(LOG, () -> channelStorage.findChannel(channelId, null));
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

        try {
            withRetries(LOG, () -> channelStorage.markEndpointUnbinding(receiverToUnbind.getUri().toString(), null));
        } catch (Exception e) {
            throw new RuntimeException("Failed to mark endpoint unbinding in storage", e);
        }

        LOG.debug("[findReceiverToUnbind] done, unbindingSender={}, found receiverToUnbind={}",
            unbindingSender.getUri(), receiverToUnbind.getUri());

        return receiverToUnbind;
    }

    @Nullable
    private Connection findConnectionToBreak(Endpoint unbindingReceiver) throws IllegalChannelGraphStateException {
        LOG.debug("[findConnectionToBreak], unbindingReceiver={}", unbindingReceiver.getUri());

        final String channelId = unbindingReceiver.getChannelId();
        final Channel channel;
        try {
            channel = withRetries(LOG, () -> channelStorage.findChannel(channelId, null));
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

        try {
            withRetries(LOG, () -> channelStorage.markConnectionDisconnecting(channelId,
                connectionToBreak.sender().getUri().toString(), connectionToBreak.receiver().getUri().toString(), null));
        } catch (Exception e) {
            throw new RuntimeException("Failed to mark connection disconnecting in storage", e);
        }

        LOG.debug("[findConnectionToBreak] done, connection found, unbindingReceiver={}, foundConnectedSender={}",
            unbindingReceiver.getUri(), connectionToBreak.sender().getUri());

        return connectionToBreak;
    }

    private boolean removeConnection(Endpoint unbindingReceiver, Endpoint connectedSender)
        throws IllegalChannelGraphStateException
    {
        LOG.debug("[removeConnection], unbindingReceiver={}, connectedSender={}",
            unbindingReceiver.getUri(), connectedSender.getUri());

        final String channelId = unbindingReceiver.getChannelId();
        final Channel channel;
        try {
            channel = withRetries(LOG, () -> channelStorage.findChannel(channelId, null));
        } catch (Exception e) {
            throw new RuntimeException("Failed to find channel in storage", e);
        }
        if (channel == null) {
            LOG.warn("[removeConnection] skipped, channel already removed, unbindingReceiver={}, connectedSender={}",
                unbindingReceiver.getUri(), connectedSender.getUri());
            return false;
        }

        final Endpoint actualStateEndpoint = channel.getEndpoint(unbindingReceiver.getUri());
        if (actualStateEndpoint == null) {
            LOG.warn("[removeConnection] skipped, receiver already removed, unbindingReceiver={}, connectedSender={}",
                unbindingReceiver.getUri(), connectedSender.getUri());
            return false;
        }

        if (actualStateEndpoint.getStatus() != Endpoint.LifeStatus.UNBINDING) {
            throw new IllegalChannelGraphStateException(channelId,
                "Endpoint " + unbindingReceiver.getUri() + " has wrong lifeStatus " + actualStateEndpoint.getStatus());
        }

        final Connection actualStateConnection = channel.getConnection(connectedSender.getUri(), unbindingReceiver.getUri());
        if (actualStateConnection == null) {
            LOG.warn("[removeConnection] skipped, connection already removed, unbindingReceiver={}, connectedSender={}",
                unbindingReceiver.getUri(), connectedSender.getUri());
            return true;
        }

        if (actualStateConnection.status() != Connection.LifeStatus.DISCONNECTING) {
            throw new IllegalChannelGraphStateException(channelId, "Connection "
                                                                   + "(sender=" + actualStateConnection.sender().getUri() + ", "
                                                                   + "receiver=" + actualStateConnection.receiver().getUri() + ") "
                                                                   + "has wrong lifeStatus " + actualStateConnection.status());
        }

        try {
            withRetries(LOG, () -> channelStorage.removeEndpointConnection(
                channelId, connectedSender.getUri().toString(), unbindingReceiver.getUri().toString(), null));
        } catch (Exception e) {
            throw new RuntimeException("Failed to remove connection in storage");
        }

        LOG.debug("[removeConnection] done, unbindingReceiver={}, connectedSender={}",
            unbindingReceiver.getUri(), connectedSender.getUri());

        return true;
    }

}
