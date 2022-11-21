package ai.lzy.channelmanager.v2.control;

import ai.lzy.channelmanager.lock.GrainedLock;
import ai.lzy.channelmanager.v2.db.ChannelStorage;
import ai.lzy.channelmanager.v2.exceptions.CancellingChannelGraphStateException;
import ai.lzy.channelmanager.v2.exceptions.ChannelGraphStateException;
import ai.lzy.channelmanager.v2.exceptions.IllegalChannelGraphStateException;
import ai.lzy.channelmanager.v2.grpc.SlotConnectionManager;
import ai.lzy.channelmanager.v2.model.Channel;
import ai.lzy.channelmanager.v2.model.Connection;
import ai.lzy.channelmanager.v2.model.Endpoint;
import ai.lzy.channelmanager.v2.slot.SlotApiClient;
import ai.lzy.model.db.exceptions.NotFoundException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.List;
import javax.annotation.Nullable;

import static ai.lzy.model.db.DbHelper.defaultRetryPolicy;
import static ai.lzy.model.db.DbHelper.withRetries;

public class ChannelControllerImpl implements ChannelController {

    private static final Logger LOG = LogManager.getLogger(ChannelControllerImpl.class);

    private final ChannelStorage channelStorage;
    private final GrainedLock lockManager;
    private final SlotConnectionManager slotConnectionManager;
    private final SlotApiClient slotApiClient;

    public ChannelControllerImpl(ChannelStorage channelStorage, GrainedLock lockManager,
                                 SlotConnectionManager slotConnectionManager, SlotApiClient slotApiClient)
    {
        this.channelStorage = channelStorage;
        this.lockManager = lockManager;
        this.slotConnectionManager = slotConnectionManager;
        this.slotApiClient = slotApiClient;
    }

    @Override
    public void bind(Endpoint endpoint) throws ChannelGraphStateException {
        while (true) {
            final Endpoint endpointToConnect;

            try (final var guard = lockManager.withLock(endpoint.channelId())) {
                endpointToConnect = findEndpointToConnect(endpoint);
                if (endpointToConnect == null) {
                    try {
                        withRetries(defaultRetryPolicy(), LOG, () ->
                            channelStorage.markEndpointBound(endpoint.uri().toString(), null));
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to mark endpoint bound in storage");
                    }
                    return;
                }
            }

            Connection potentialConnection = Connection.of(endpoint, endpointToConnect);
            slotApiClient.connect(potentialConnection.sender(), potentialConnection.receiver(), Duration.ofSeconds(10));

            boolean connectionSaved;
            try (final var guard = lockManager.withLock(endpoint.channelId())) {
                connectionSaved = saveConnection(endpoint, endpointToConnect);
            } catch (CancellingChannelGraphStateException e) {
                LOG.info("[bind] disconnecting endpoints after CancellingException, sender={}, receiver={}",
                    potentialConnection.sender().uri(), potentialConnection.receiver().uri());
                slotApiClient.disconnect(potentialConnection.receiver());
                LOG.info("[bind] endpoints disconnected after CancellingException, sender={}, receiver={}",
                    potentialConnection.sender().uri(), potentialConnection.receiver().uri());
                throw e;
            }

            if (!connectionSaved) {
                LOG.info("[bind] disconnecting endpoints after saving connection fail, sender={}, receiver={}",
                    potentialConnection.sender().uri(), potentialConnection.receiver().uri());
                slotApiClient.disconnect(potentialConnection.receiver());
                LOG.info("[bind] endpoints disconnected after saving connection fail, sender={}, receiver={}",
                    potentialConnection.sender().uri(), potentialConnection.receiver().uri());
            }
        }
    }

    @Override
    public void unbindSender(Endpoint sender) throws ChannelGraphStateException {
        final String channelId = sender.channelId();
        while (true) {
            final Endpoint receiverToUnbind;
            try (final var guard = lockManager.withLock(sender.channelId())) {
                receiverToUnbind = findReceiverToUnbind(sender);
                if (receiverToUnbind == null) {
                    break;
                }
            }

            LOG.warn("[unbindSender], found bound receiver, need force unbind, sender={}, receiver={}",
                sender.uri(), receiverToUnbind.uri());
            unbindReceiver(receiverToUnbind);
        }

        LOG.info("[unbindSender] disconnecting endpoint, sender={}", sender.uri());
        slotApiClient.disconnect(sender);
        LOG.info("[unbindSender] endpoint disconnected, sender={}", sender.uri());

        // TODO (lindvv): maybe should suspend, so shouldn't destroy, fix after slots refactoring
        LOG.info("[unbindSender] destroying endpoint, sender={}", sender.uri());
        slotApiClient.destroy(sender);
        sender.invalidate();
        LOG.info("[unbindSender] endpoint destroyed, sender={}", sender.uri());

        try (final var guard = lockManager.withLock(channelId)) {
            withRetries(defaultRetryPolicy(), LOG,
                () -> channelStorage.removeEndpointWithoutConnections(sender.uri().toString(), null));
        } catch (NotFoundException e) {
            LOG.info("[unbindSender] removing endpoint skipped, sender already removed, sender={}", sender.uri());
        } catch (Exception e) {
            throw new RuntimeException("Failed to remove endpoint in storage");
        }
    }

    @Override
    public void unbindReceiver(Endpoint receiver) throws ChannelGraphStateException {
        final String channelId = receiver.channelId();

        final Connection connectionToBreak;
        try (final var guard = lockManager.withLock(channelId)) {
            connectionToBreak = findConnectionToBreak(receiver);
            if (connectionToBreak == null) {
                try {
                    withRetries(defaultRetryPolicy(), LOG, () ->
                        channelStorage.removeEndpointWithoutConnections(receiver.uri().toString(), null));
                } catch (Exception e) {
                    throw new RuntimeException("Failed to remove endpoint in storage");
                }
                return;
            }
        }

        LOG.info("[unbindReceiver] disconnecting endpoint, receiver={}", receiver.uri());
        slotApiClient.disconnect(receiver);
        LOG.info("[unbindReceiver] endpoint disconnected, receiver={}", receiver.uri());

        // TODO (lindvv): maybe should suspend, so shouldn't destroy, fix after slots refactoring
        LOG.info("[unbindReceiver] destroying endpoint, receiver={}", receiver.uri());
        slotApiClient.destroy(receiver);
        receiver.invalidate();
        LOG.info("[unbindReceiver] endpoint destroyed, receiver={}", receiver.uri());

        try (final var guard = lockManager.withLock(channelId)) {
            boolean connectionRemoved = removeConnection(connectionToBreak.receiver(), connectionToBreak.sender());
            if (connectionRemoved) {
                try {
                    withRetries(defaultRetryPolicy(), LOG, () ->
                        channelStorage.removeEndpointWithoutConnections(receiver.uri().toString(), null));
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
                channel = withRetries(defaultRetryPolicy(), LOG, () ->
                    channelStorage.findChannel(channelId, Channel.LifeStatus.DESTROYING, null));
            } catch (Exception e) {
                throw new RuntimeException("Failed to find destroying channel in storage", e);
            }
            if (channel == null) {
                LOG.info("[destroy] skipped, channel {} already removed", channelId);
                return;
            }
            try {
                withRetries(defaultRetryPolicy(), LOG, () ->
                    channelStorage.markAllEndpointsUnbinding(channelId, null));
            } catch (Exception e) {
                throw new RuntimeException("Failed to mark channel endpoints unbinding in storage", e);
            }
        }

        for (Endpoint receiver : channel.existedReceivers()) {
            unbindReceiver(receiver);
        }

        for (Endpoint sender : channel.existedSenders()) {
            unbindSender(sender);
        }

        try (final var guard = lockManager.withLock(channelId)) {
            withRetries(defaultRetryPolicy(), LOG, () -> channelStorage.removeChannel(channelId, null));
        } catch (NotFoundException e) {
            LOG.info("[destroy] removing channel skipped, channel {} already removed", channelId);
        } catch (Exception e) {
            throw new RuntimeException("Failed to remove channel in storage", e);
        }
    }

    @Nullable
    private Endpoint findEndpointToConnect(Endpoint bindingEndpoint) throws CancellingChannelGraphStateException {
        LOG.debug("[findEndpointToConnect], bindingEndpoint={}", bindingEndpoint.uri());

        final String channelId = bindingEndpoint.channelId();
        final Channel channel;
        try {
            channel = withRetries(defaultRetryPolicy(), LOG, () ->
                channelStorage.findChannel(channelId, Channel.LifeStatus.ALIVE, null));
        } catch (Exception e) {
            throw new RuntimeException("Failed to find channel in storage", e);
        }
        if (channel == null) {
            throw new CancellingChannelGraphStateException(channelId, "Channel not found");
        }

        final Endpoint actualStateEndpoint = channel.endpoint(bindingEndpoint.uri());
        if (actualStateEndpoint == null) {
            throw new CancellingChannelGraphStateException(channelId,
                "Endpoint " + bindingEndpoint.uri() + " not found");
        }

        if (actualStateEndpoint.status() != Endpoint.LifeStatus.BINDING) {
            throw new CancellingChannelGraphStateException(channelId,
                "Endpoint " + bindingEndpoint.uri() + " has wrong lifeStatus " + actualStateEndpoint.status());
        }

        final Endpoint endpointToConnect;
        final Endpoint sender, receiver;
        switch (bindingEndpoint.slotDirection()) {
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
                "Endpoint " + bindingEndpoint.uri() + " has unexpected direction");
        }

        if (endpointToConnect == null) {
            LOG.debug("[findEndpointToConnect] done, nothing to connect, bindingEndpoint={}", bindingEndpoint.uri());
            return null;
        }

        try {
            withRetries(defaultRetryPolicy(), LOG, () ->
                channelStorage.insertConnection(channelId, Connection.of(sender, receiver), null));
        } catch (Exception e) {
            throw new RuntimeException("Failed to insert connection in storage", e);
        }

        LOG.debug("[findEndpointToConnect] done, bindingEndpoint={}, found endpointToConnect={}",
            bindingEndpoint.uri(), endpointToConnect.uri());

        return endpointToConnect;
    }

    private boolean saveConnection(Endpoint bindingEndpoint, Endpoint connectedEndpoint)
        throws CancellingChannelGraphStateException
    {
        LOG.debug("[saveConnection], bindingEndpoint={}, connectedEndpoint={}",
            bindingEndpoint.uri(), connectedEndpoint.uri());

        final String channelId = bindingEndpoint.channelId();
        final Endpoint sender, receiver;
        switch (bindingEndpoint.slotDirection()) {
            case OUTPUT /* SENDER */ -> {
                sender = bindingEndpoint;
                receiver = connectedEndpoint;
            }
            case INPUT /* RECEIVER */ -> {
                sender = connectedEndpoint;
                receiver = bindingEndpoint;
            }
            default -> throw new IllegalStateException(
                "Endpoint " + bindingEndpoint.uri() + " has unexpected direction" + bindingEndpoint.slotDirection());
        }

        final Channel channel;
        try {
            channel = withRetries(defaultRetryPolicy(), LOG, () ->
                channelStorage.findChannel(channelId, Channel.LifeStatus.ALIVE, null));
        } catch (Exception e) {
            throw new RuntimeException("Failed to find channel in storage", e);
        }
        if (channel == null) {
            throw new CancellingChannelGraphStateException(channelId, "Channel not found");
        }

        final Endpoint actualStateEndpoint = channel.endpoint(bindingEndpoint.uri());
        if (actualStateEndpoint == null) {
            throw new CancellingChannelGraphStateException(channelId,
                "Endpoint " + bindingEndpoint.uri() + " not found");
        }

        if (actualStateEndpoint.status() != Endpoint.LifeStatus.BINDING) {
            throw new CancellingChannelGraphStateException(channelId,
                "Endpoint " + bindingEndpoint.uri() + " has wrong lifeStatus " + actualStateEndpoint.status());
        }

        final Connection actualStateConnection = channel.connection(sender.uri(), receiver.uri());
        if (actualStateConnection == null || actualStateConnection.status() != Connection.LifeStatus.CONNECTING) {
            String connectionStatus = actualStateConnection == null ? "null" : actualStateConnection.status().name();
            LOG.warn("[saveConnection] skipped, unexpected connection status {}, "
                      + "bindingEndpoint={}, connectedEndpoint={}",
                connectionStatus, bindingEndpoint.uri(), connectedEndpoint.uri());
            return false;
        }

        try {
            withRetries(defaultRetryPolicy(), LOG, () -> channelStorage.markConnectionAlive(
                channelId, sender.uri().toString(), receiver.uri().toString(), null));
        } catch (Exception e) {
            throw new RuntimeException("Failed to save alive connection in storage", e);
        }

        LOG.debug("[saveConnection] done, bindingEndpoint={}, connectedEndpoint={}",
            bindingEndpoint.uri().toString(), connectedEndpoint.uri().toString());

        return true;
    }

    @Nullable
    private Endpoint findReceiverToUnbind(Endpoint unbindingSender) throws IllegalChannelGraphStateException {
        LOG.debug("[findReceiverToUnbind], unbindingSender={}", unbindingSender.uri());

        final String channelId = unbindingSender.channelId();
        final Channel channel;
        try {
            channel = withRetries(defaultRetryPolicy(), LOG, () -> channelStorage.findChannel(channelId, null));
        } catch (Exception e) {
            throw new RuntimeException("Failed to find channel in storage", e);
        }
        if (channel == null) {
            LOG.warn("[findReceiverToUnbind] skipped, channel already removed, unbindingSender={}",
                unbindingSender.uri());
            return null;
        }

        final Endpoint actualStateEndpoint = channel.endpoint(unbindingSender.uri());
        if (actualStateEndpoint == null) {
            LOG.warn("[findReceiverToUnbind] skipped, sender already removed, unbindingSender={}",
                unbindingSender.uri());
            return null;
        }

        if (actualStateEndpoint.status() != Endpoint.LifeStatus.UNBINDING) {
            throw new IllegalChannelGraphStateException(channelId,
                "Endpoint " + unbindingSender.uri() + " has wrong lifeStatus " + actualStateEndpoint.status());
        }

        final List<Connection> actualStateConnections = channel.connectionsOfEndpoint(unbindingSender.uri());
        final Endpoint receiverToUnbind = actualStateConnections.stream()
            .filter(conn -> conn.status() == Connection.LifeStatus.CONNECTED)
            .map(Connection::receiver)
            .findFirst().orElse(null);

        if (receiverToUnbind == null) {
            LOG.debug("[findReceiverToUnbind] done, nothing to unbind, unbindingSender={}", unbindingSender.uri());
            return null;
        }

        try {
            withRetries(defaultRetryPolicy(), LOG, () ->
                channelStorage.markEndpointUnbinding(receiverToUnbind.uri().toString(), null));
        } catch (Exception e) {
            throw new RuntimeException("Failed to mark endpoint unbinding in storage", e);
        }

        LOG.debug("[findReceiverToUnbind] done, unbindingSender={}, found receiverToUnbind={}",
            unbindingSender.uri(), receiverToUnbind.uri());

        return receiverToUnbind;
    }

    @Nullable
    private Connection findConnectionToBreak(Endpoint unbindingReceiver) throws IllegalChannelGraphStateException {
        LOG.debug("[findConnectionToBreak], unbindingReceiver={}", unbindingReceiver.uri());

        final String channelId = unbindingReceiver.channelId();
        final Channel channel;
        try {
            channel = withRetries(defaultRetryPolicy(), LOG, () -> channelStorage.findChannel(channelId, null));
        } catch (Exception e) {
            throw new RuntimeException("Failed to find channel in storage", e);
        }
        if (channel == null) {
            LOG.warn("[findConnectionToBreak] skipped, channel already removed, unbindingReceiver={}",
                unbindingReceiver.uri());
            return null;
        }

        final Endpoint actualStateEndpoint = channel.endpoint(unbindingReceiver.uri());
        if (actualStateEndpoint == null) {
            LOG.warn("[findConnectionToBreak] skipped, receiver already removed, unbindingReceiver={}",
                unbindingReceiver.uri());
            return null;
        }

        if (actualStateEndpoint.status() != Endpoint.LifeStatus.UNBINDING) {
            throw new IllegalChannelGraphStateException(channelId,
                "Endpoint " + unbindingReceiver.uri() + " has wrong lifeStatus " + actualStateEndpoint.status());
        }

        final List<Connection> actualStateConnections = channel.connectionsOfEndpoint(unbindingReceiver.uri());
        final Connection connectionToBreak = actualStateConnections.stream()
            .filter(it -> it.status() == Connection.LifeStatus.DISCONNECTING)
            .findFirst().orElse(null);

        if (connectionToBreak == null) {
            LOG.debug("[findConnectionToBreak] done, nothing to disconnect, unbindingReceiver={}",
                unbindingReceiver.uri());
            return null;
        }

        try {
            withRetries(defaultRetryPolicy(), LOG, () -> channelStorage.markConnectionDisconnecting(channelId,
                connectionToBreak.sender().uri().toString(), connectionToBreak.receiver().uri().toString(), null));
        } catch (Exception e) {
            throw new RuntimeException("Failed to mark connection disconnecting in storage", e);
        }

        LOG.debug("[findConnectionToBreak] done, connection found, unbindingReceiver={}, foundConnectedSender={}",
            unbindingReceiver.uri(), connectionToBreak.sender().uri());

        return connectionToBreak;
    }

    private boolean removeConnection(Endpoint unbindingReceiver, Endpoint connectedSender)
        throws IllegalChannelGraphStateException
    {
        LOG.debug("[removeConnection], unbindingReceiver={}, connectedSender={}",
            unbindingReceiver.uri(), connectedSender.uri());

        final String channelId = unbindingReceiver.channelId();
        final Channel channel;
        try {
            channel = withRetries(defaultRetryPolicy(), LOG, () -> channelStorage.findChannel(channelId, null));
        } catch (Exception e) {
            throw new RuntimeException("Failed to find channel in storage", e);
        }
        if (channel == null) {
            LOG.warn("[removeConnection] skipped, channel already removed, unbindingReceiver={}, connectedSender={}",
                unbindingReceiver.uri(), connectedSender.uri());
            return false;
        }

        final Endpoint actualStateEndpoint = channel.endpoint(unbindingReceiver.uri());
        if (actualStateEndpoint == null) {
            LOG.warn("[removeConnection] skipped, receiver already removed, unbindingReceiver={}, connectedSender={}",
                unbindingReceiver.uri(), connectedSender.uri());
            return false;
        }

        if (actualStateEndpoint.status() != Endpoint.LifeStatus.UNBINDING) {
            throw new IllegalChannelGraphStateException(channelId,
                "Endpoint " + unbindingReceiver.uri() + " has wrong lifeStatus " + actualStateEndpoint.status());
        }

        final Connection actualStateConnection = channel.connection(connectedSender.uri(), unbindingReceiver.uri());
        if (actualStateConnection == null) {
            LOG.warn("[removeConnection] skipped, connection already removed, unbindingReceiver={}, connectedSender={}",
                unbindingReceiver.uri(), connectedSender.uri());
            return true;
        }

        if (actualStateConnection.status() != Connection.LifeStatus.DISCONNECTING) {
            throw new IllegalChannelGraphStateException(channelId, "Connection "
                + "(sender=" + actualStateConnection.sender().uri() + ", "
                + "receiver=" + actualStateConnection.receiver().uri() + ") "
                + "has wrong lifeStatus " + actualStateConnection.status());
        }

        try {
            withRetries(defaultRetryPolicy(), LOG, () -> channelStorage.removeEndpointConnection(
                channelId, connectedSender.uri().toString(), unbindingReceiver.uri().toString(), null));
        } catch (Exception e) {
            throw new RuntimeException("Failed to remove connection in storage");
        }

        LOG.debug("[removeConnection] done, unbindingReceiver={}, connectedSender={}",
            unbindingReceiver.uri(), connectedSender.uri());

        return true;
    }

}
