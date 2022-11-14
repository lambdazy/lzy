package ai.lzy.channelmanager.v2.control;

import ai.lzy.channelmanager.lock.GrainedLock;
import ai.lzy.channelmanager.v2.db.ChannelStorage;
import ai.lzy.channelmanager.v2.grpc.SlotConnectionManager;
import ai.lzy.channelmanager.v2.model.Channel;
import ai.lzy.channelmanager.v2.model.Connection;
import ai.lzy.channelmanager.v2.model.Endpoint;
import ai.lzy.channelmanager.v2.slot.SlotApiClient;
import ai.lzy.longrunning.Operation;
import ai.lzy.v1.channel.v2.LCMS;
import com.google.protobuf.Any;
import io.grpc.Status;
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
    public void executeBind(Endpoint endpoint, Operation bindOperation) {
        LOG.debug("[executeBind], endpoint={}", endpoint.uri());

        try {
            bind(endpoint);
        } catch (CancellingStateException e) {
            LOG.warn("[executeBind] operation {} cancelled, " + e.getMessage());
            bindOperation.setError(Status.CANCELLED);
            operationStorage.update(operation);
        }  catch (Exception e) {
            LOG.error("[executeBind] operation {} failed, " + e.getMessage());
            // error op
        }

        bindOperation.setResponse(Any.pack(LCMS.BindRequest.getDefaultInstance()));
        operationStorage.update(bindOperation);

        LOG.debug("[executeBind] done, endpoint={}", endpoint.uri());

    }

    @Override
    public void executeUnbind(Endpoint endpoint) {
        LOG.debug("[executeUnbind], endpoint={}, channel={}", endpoint.uri(), channel.id());

        for (final Endpoint connectedEndpoint : channel.connections().ofEndpoint(endpoint)) {

        }
    }

    private void bind(Endpoint endpoint) throws Exception {
        while (true) {
            final Endpoint endpointToConnect = findEndpointToConnect(endpoint);

            if (endpointToConnect == null) {
                return;
            }

            Connection potentialConnection = Connection.of(endpoint, endpointToConnect);
            slotApiClient.connect(potentialConnection.sender(), potentialConnection.receiver(), Duration.ofSeconds(10));

            boolean connectionSaved;
            try {
                connectionSaved = saveConnection(endpoint, endpointToConnect);
            } catch (CancellingStateException e) {
                slotApiClient.disconnect(potentialConnection.receiver());
                throw e;
            }

            if (!connectionSaved) {
                slotApiClient.disconnect(potentialConnection.receiver());
            }
        }
    }

    private void unbindSender(Endpoint endpoint) throws Exception {
        final String channelId = endpoint.channelId();
        while (true) {
            final Connection connectionToBreak;
            try (final var guard = lockManager.withLock(channelId)) {
                final Channel channel = channelStorage.findChannel(channelId, ChannelStorage.ChannelLifeStatus.ALIVE, null);

                if (channel == null) {
                    // ok, already removed
                }

                final Endpoint actualStateEndpoint = channel.endpoint(endpoint.uri());
                if (actualStateEndpoint == null) {
                    // ok, already removed
                }

                if (actualStateEndpoint.lifeStatus() != Endpoint.LifeStatus.UNBINDING) {
                    // unexpected , mark error
                }

                final List<Connection> actualStateConnections = channel.connections(endpoint.uri());
                connectionToBreak = actualStateConnections.stream()
                    .filter(it -> it.status() == Connection.LifeStatus.CONNECTED)
                    .findFirst().orElse(null);

                if (connectionToBreak == null) {
                    break;
                }

                // log warning, force unbinding receiver
                withRetries(defaultRetryPolicy(), LOG, () -> channelStorage.markEndpointUnbinding(
                    connectionToBreak.receiver().uri().toString(), null));
            }

            unbindReceiver(connectionToBreak.receiver());
        }

        slotApiClient.disconnect(endpoint);

        try (final var guard = lockManager.withLock(channelId)) {
            withRetries(defaultRetryPolicy(), LOG,
                () -> channelStorage.removeEndpointWithoutConnections(endpoint.uri().toString(), null));
        }
    }

    private void unbindReceiver(Endpoint endpoint) throws Exception {
        final String channelId = endpoint.channelId();
        final Connection connectionToBreak;
        try (final var guard = lockManager.withLock(channelId)) {
            final Channel channel = channelStorage.findChannel(channelId, ChannelStorage.ChannelLifeStatus.ALIVE, null);

            if (channel == null) {
                // ok, already removed
            }

            final Endpoint actualStateEndpoint = channel.endpoint(endpoint.uri());
            if (actualStateEndpoint == null) {
                // ok, already removed
            }

            if (actualStateEndpoint.lifeStatus() != Endpoint.LifeStatus.UNBINDING) {
                // unexpected , mark error
            }

            final List<Connection> actualStateConnections = channel.connections(endpoint.uri());
            connectionToBreak = actualStateConnections.stream()
                .filter(it -> it.status() == Connection.LifeStatus.CONNECTED)
                .findFirst().orElse(null);

            if (connectionToBreak == null) {
                withRetries(defaultRetryPolicy(), LOG, () -> channelStorage.removeEndpointWithoutConnections(endpoint.uri().toString(), null));
                return;
            }

            withRetries(defaultRetryPolicy(), LOG, () -> channelStorage.markEndpointConnectionDisconnecting(
                channelId, connectionToBreak.sender().uri().toString(), connectionToBreak.receiver().uri().toString(), null));
        }

        slotApiClient.disconnect(endpoint);

        try (final var guard = lockManager.withLock(channelId)) {
            final Channel channel = channelStorage.findChannel(channelId, ChannelStorage.ChannelLifeStatus.ALIVE, null);

            if (channel == null) {
                // ok, already removed
            }

            final Endpoint actualStateEndpoint = channel.endpoint(endpoint.uri());
            if (actualStateEndpoint == null) {
                // ok, already removed
            }

            if (actualStateEndpoint.lifeStatus() != Endpoint.LifeStatus.UNBINDING) {
                // unexpected, mark error
            }

            final Connection actualStateConnection = channel.connection(connectionToBreak.sender().uri(), connectionToBreak.receiver().uri());
            if (actualStateConnection == null) {
                // ok, already removed connection, to remove endpoint
                withRetries(defaultRetryPolicy(), LOG, () -> channelStorage.removeEndpointWithoutConnections(endpoint.uri().toString(), null));
            }

            if (actualStateConnection.status() != Connection.LifeStatus.DISCONNECTING) {
                // unexpected, mark error
            }

            withRetries(defaultRetryPolicy(), LOG, () -> channelStorage.removeEndpointConnection(
                channelId, connectionToBreak.sender().uri().toString(), connectionToBreak.receiver().uri().toString(), null));

            withRetries(defaultRetryPolicy(), LOG, () -> channelStorage.removeEndpointWithoutConnections(endpoint.uri().toString(), null));
        }

    }

    @Nullable
    private Endpoint findEndpointToConnect(Endpoint endpoint) throws Exception {
        final String channelId = endpoint.channelId();
        try (final var guard = lockManager.withLock(channelId)) {
            final Channel channel = channelStorage.findChannel(channelId, ChannelStorage.ChannelLifeStatus.ALIVE, null);

            if (channel == null) {
                throw new CancellingStateException("Channel " + channelId + " not found");
            }

            final Endpoint actualStateEndpoint = channel.endpoint(endpoint.uri());
            if (actualStateEndpoint == null || actualStateEndpoint.lifeStatus() != Endpoint.LifeStatus.BINDING) {
                throw new CancellingStateException("Endpoint " + endpoint.uri() + " has wrong lifeStatus");
            }

            final Endpoint endpointToConnect;
            final Endpoint sender, receiver;
            switch (endpoint.slotDirection()) {
                case OUTPUT /* SENDER */ -> {
                    endpointToConnect = channel.findSenderToConnect(endpoint);
                    sender = endpoint;
                    receiver = endpointToConnect;
                }
                case INPUT /* RECEIVER */ -> {
                    endpointToConnect = channel.findReceiversToConnect(endpoint).stream().findFirst().orElse(null);
                    sender = endpointToConnect;
                    receiver = endpoint;
                }
                default -> throw new IllegalStateException("Endpoint " + endpoint.uri() + " has unexpected direction");
            }

            if (endpointToConnect == null) {
                withRetries(defaultRetryPolicy(), LOG, () -> channelStorage.markEndpointBound(endpoint.uri().toString(), null));
                return null;
            }

            withRetries(defaultRetryPolicy(), LOG, () -> channelStorage.insertEndpointConnection(
                channelId, sender.uri().toString(), receiver.uri().toString(), null));

            return endpointToConnect;
        }
    }

    private boolean saveConnection(Endpoint endpoint, Endpoint connectedEndpoint) throws Exception {
        final String channelId = endpoint.channelId();

        final Endpoint sender, receiver;
        switch (endpoint.slotDirection()) {
            case OUTPUT /* SENDER */ -> {
                sender = endpoint;
                receiver = connectedEndpoint;
            }
            case INPUT /* RECEIVER */ -> {
                sender = connectedEndpoint;
                receiver = endpoint;
            }
            default -> throw new IllegalStateException("Endpoint " + endpoint.uri() + " has unexpected direction" + endpoint.slotDirection());
        }

        try (final var guard = lockManager.withLock(channelId)) {
            Channel channel = channelStorage.findChannel(channelId, ChannelStorage.ChannelLifeStatus.ALIVE, null);

            if (channel == null) {
                throw new CancellingStateException("Channel " + channelId + " not found");
            }

            final Endpoint actualStateEndpoint = channel.endpoint(endpoint.uri());
            if (actualStateEndpoint == null || actualStateEndpoint.lifeStatus() != Endpoint.LifeStatus.BINDING) {
                throw new CancellingStateException("Endpoint " + endpoint.uri() + " has wrong lifeStatus");
            }

            final Connection actualStateConnection = channel.connection(sender.uri(), receiver.uri());
            if (actualStateConnection == null || actualStateConnection.status() != Connection.LifeStatus.CONNECTING) {
                return false;
            }

            withRetries(defaultRetryPolicy(), LOG, () -> channelStorage.markEndpointConnectionAlive(
                channelId, sender.uri().toString(), receiver.uri().toString(), null));
        }

        return true;
    }

    private static class CancellingStateException extends Exception {

        public CancellingStateException(String message) {
            super(message);
        }

        @Override
        public synchronized Throwable fillInStackTrace() {
            return this;
        }
    }

}
