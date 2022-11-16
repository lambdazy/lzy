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

import java.sql.SQLException;
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
                    withRetries(defaultRetryPolicy(), LOG, () ->
                        channelStorage.markEndpointBound(endpoint.uri().toString(), null));
                    return;
                }
            }

            Connection potentialConnection = Connection.of(endpoint, endpointToConnect);
            slotApiClient.connect(potentialConnection.sender(), potentialConnection.receiver(), Duration.ofSeconds(10));

            boolean connectionSaved;
            try (final var guard = lockManager.withLock(endpoint.channelId())) {
                connectionSaved = saveConnection(endpoint, endpointToConnect);
            } catch (CancellingChannelGraphStateException e) {
                slotApiClient.disconnect(potentialConnection.receiver());
                throw e;
            }

            if (!connectionSaved) {
                slotApiClient.disconnect(potentialConnection.receiver());
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

            // log warn, force unbind receiver
            unbindReceiver(receiverToUnbind);
        }

        slotApiClient.disconnect(sender);

        // TODO (lindvv): maybe should suspend, so shouldn't destroy, fix after slots refactoring
        slotApiClient.destroy(sender);

        try (final var guard = lockManager.withLock(channelId)) {
            withRetries(defaultRetryPolicy(), LOG,
                () -> channelStorage.removeEndpointWithoutConnections(sender.uri().toString(), null));
        } catch (NotFoundException e) {
            // ok, already removed
        }
    }

    @Override
    public void unbindReceiver(Endpoint receiver) throws ChannelGraphStateException {
        final String channelId = receiver.channelId();

        final Connection connectionToBreak;
        try (final var guard = lockManager.withLock(channelId)) {
            connectionToBreak = findConnectionToBreak(receiver);
            if (connectionToBreak == null) {
                withRetries(defaultRetryPolicy(), LOG, () ->
                    channelStorage.removeEndpointWithoutConnections(receiver.uri().toString(), null));
                return;
            }
        }

        slotApiClient.disconnect(receiver);

        // TODO (lindvv): maybe should suspend, so shouldn't destroy, fix after slots refactoring
        slotApiClient.destroy(receiver);

        try (final var guard = lockManager.withLock(channelId)) {
            boolean connectionRemoved = removeConnection(connectionToBreak.receiver(), connectionToBreak.sender());
            if (connectionRemoved) {
                withRetries(defaultRetryPolicy(), LOG, () ->
                    channelStorage.removeEndpointWithoutConnections(receiver.uri().toString(), null));
            }
        }

    }

    @Override
    public void destroy(String channelId) throws ChannelGraphStateException {

        final Channel channel;
        try (final var guard = lockManager.withLock(channelId)) {
            channel = channelStorage.findChannel(channelId, Channel.LifeStatus.DESTROYING, null);
            if (channel == null) {
                // already destroyed
                return;
            }

            withRetries(defaultRetryPolicy(), LOG, () -> channelStorage.markAllEndpointsUnbinding(channelId, null));
        }

        for (Endpoint receiver : channel.existedReceivers()) {
            unbindReceiver(receiver);
        }

        for (Endpoint sender : channel.existedSenders()) {
            unbindSender(sender);
        }

        try (final var guard = lockManager.withLock(channelId)) {
            withRetries(defaultRetryPolicy(), LOG, () -> channelStorage.removeChannel(channelId, null));
        }
    }

    @Nullable
    private Endpoint findEndpointToConnect(Endpoint bindingEndpoint) throws ChannelGraphStateException {
        LOG.debug("[findEndpointToConnect], bindingEndpoint={}", bindingEndpoint.uri());

        final String channelId = bindingEndpoint.channelId();
        final Channel channel;
        try {
            channel = channelStorage.findChannel(channelId, Channel.LifeStatus.ALIVE, null);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to find channel in storage", e); // EXCEPTION_TYPE: request failed
        }
        if (channel == null) {
            throw new CancellingChannelGraphStateException(channelId, "Channel not found");
        }

        final Endpoint actualStateEndpoint = channel.endpoint(bindingEndpoint.uri());
        if (actualStateEndpoint == null) {
            throw new CancellingChannelGraphStateException(channelId, "Endpoint " + bindingEndpoint.uri() + " not found");
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
            default -> throw new IllegalStateException("Endpoint " + bindingEndpoint.uri() + " has unexpected direction");
        }

        if (endpointToConnect == null) {
            return null;
        }

        withRetries(defaultRetryPolicy(), LOG, () ->
            channelStorage.insertConnection(channelId, Connection.of(sender, receiver), null));

        LOG.debug("[findEndpointToConnect] done, bindingEndpoint={}, found endpointToConnect={}",
            bindingEndpoint.uri(), endpointToConnect.uri());

        return endpointToConnect;
    }

    private boolean saveConnection(Endpoint bindingEndpoint, Endpoint connectedEndpoint) throws Exception {
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

        Channel channel = channelStorage.findChannel(channelId, Channel.LifeStatus.ALIVE, null);
        if (channel == null) {
            throw new CancellingChannelGraphStateException(channelId, "Channel not found");
        }

        final Endpoint actualStateEndpoint = channel.endpoint(bindingEndpoint.uri());
        if (actualStateEndpoint == null) {
            throw new CancellingChannelGraphStateException(channelId, "Endpoint " + bindingEndpoint.uri() + " not found");
        }

        if (actualStateEndpoint.status() != Endpoint.LifeStatus.BINDING) {
            throw new CancellingChannelGraphStateException(channelId,
                "Endpoint " + bindingEndpoint.uri() + " has wrong lifeStatus " + actualStateEndpoint.status());
        }

        final Connection actualStateConnection = channel.connection(sender.uri(), receiver.uri());
        if (actualStateConnection == null || actualStateConnection.status() != Connection.LifeStatus.CONNECTING) {
            return false;
        }

        withRetries(defaultRetryPolicy(), LOG, () -> channelStorage.markConnectionAlive(
            channelId, sender.uri().toString(), receiver.uri().toString(), null));
        return true;
    }

    @Nullable
    private Endpoint findReceiverToUnbind(Endpoint unbindingSender) throws Exception {
        final String channelId = unbindingSender.channelId();

        final Channel channel = channelStorage.findChannel(channelId, null);
        if (channel == null) {
            // log warn, already removed
            return null;
        }

        final Endpoint actualStateEndpoint = channel.endpoint(unbindingSender.uri());
        if (actualStateEndpoint == null) {
            // log warn, already removed
            return null;
        }

        if (actualStateEndpoint.status() != Endpoint.LifeStatus.UNBINDING) {
            throw new IllegalChannelGraphStateException(channelId,
                "Endpoint " + unbindingSender.uri() + " has wrong lifeStatus " + actualStateEndpoint.status());
        }

        final List<Connection> actualStateConnections = channel.connections(unbindingSender.uri());
        final Endpoint receiverToUnbind = actualStateConnections.stream()
            .filter(conn -> conn.status() == Connection.LifeStatus.CONNECTED)
            .map(Connection::receiver)
            .findFirst().orElse(null);

        if (receiverToUnbind == null) {
            return null;
        }

        // log warning, force unbinding receiver
        withRetries(defaultRetryPolicy(), LOG, () -> channelStorage.markEndpointUnbinding(
            receiverToUnbind.uri().toString(), null));

        return receiverToUnbind;
    }

    @Nullable
    private Connection findConnectionToBreak(Endpoint unbindingReceiver) throws Exception {
        final String channelId = unbindingReceiver.channelId();

        final Channel channel = channelStorage.findChannel(channelId, null);
        if (channel == null) {
            // TODO log warn, already removed
            return null;
        }

        final Endpoint actualStateEndpoint = channel.endpoint(unbindingReceiver.uri());
        if (actualStateEndpoint == null) {
            // TODO log warn, already removed
            return null;
        }

        if (actualStateEndpoint.status() != Endpoint.LifeStatus.UNBINDING) {
            throw new IllegalChannelGraphStateException(channelId, ""); // TODO
        }

        final List<Connection> actualStateConnections = channel.connections(unbindingReceiver.uri());
        final Connection connectionToBreak = actualStateConnections.stream()
            .filter(it -> it.status() == Connection.LifeStatus.DISCONNECTING)
            .findFirst().orElse(null);

        if (connectionToBreak == null) {
            return null;
        }

        withRetries(defaultRetryPolicy(), LOG, () -> channelStorage.markConnectionDisconnecting(
            channelId, connectionToBreak.sender().uri().toString(), connectionToBreak.receiver().uri().toString(), null));

        return connectionToBreak;
    }

    private boolean removeConnection(Endpoint unbindingReceiver, Endpoint connectedSender) throws Exception {
        final String channelId = unbindingReceiver.channelId();

        final Channel channel = channelStorage.findChannel(channelId, null);
        if (channel == null) {
            // ok, already removed
            return false;
        }

        final Endpoint actualStateEndpoint = channel.endpoint(unbindingReceiver.uri());
        if (actualStateEndpoint == null) {
            // ok, already removed
            return false;
        }

        if (actualStateEndpoint.status() != Endpoint.LifeStatus.UNBINDING) {
            throw new IllegalChannelGraphStateException(channelId, ""); // TODO
        }

        final Connection actualStateConnection = channel.connection(connectedSender.uri(), unbindingReceiver.uri());
        if (actualStateConnection == null) {
            // log warn
            return true;
        }

        if (actualStateConnection.status() != Connection.LifeStatus.DISCONNECTING) {
            throw new IllegalChannelGraphStateException(channelId, ""); // TODO
        }

        withRetries(defaultRetryPolicy(), LOG, () -> channelStorage.removeEndpointConnection(
            channelId, connectedSender.uri().toString(), unbindingReceiver.uri().toString(), null));

        return true;
    }

}
