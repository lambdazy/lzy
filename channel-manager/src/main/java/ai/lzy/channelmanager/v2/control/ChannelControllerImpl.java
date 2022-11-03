package ai.lzy.channelmanager.v2.control;

import ai.lzy.channelmanager.lock.GrainedLock;
import ai.lzy.channelmanager.v2.db.ChannelStorage;
import ai.lzy.channelmanager.v2.grpc.SlotConnectionManager;
import ai.lzy.channelmanager.v2.model.Channel;
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
    public void executeBind(Endpoint endpoint) {
        LOG.debug("[executeBind], endpoint={}", endpoint.uri());


    }

    @Override
    public void executeUnbind(Channel channel, Endpoint endpoint) {
        LOG.debug("[executeUnbind], endpoint={}, channel={}", endpoint.uri(), channel.id());

        for (final Endpoint connectedEndpoint : channel.connections().ofEndpoint(endpoint)) {

        }
    }

    private void bindSender(Endpoint endpoint, Operation bindOperation) {
        final String channelId = endpoint.channelId();

        boolean toCancelOperation = false;
        Endpoint receiverToUndoConnection;
        while (true) {
            final Endpoint receiverToConnect;
            receiverToUndoConnection = null;

            try (final var guard = lockManager.withLock(channelId)) {
                Channel channel = channelStorage.findChannel(channelId, ChannelStorage.ChannelLifeStatus.ALIVE, null);

                if (channel == null) {
                    LOG.warn("[bindSender] operation {} cancelled, channel {} not found",
                        bindOperation.id(), channelId);
                    toCancelOperation = true;
                    break;
                }

                Endpoint sender = channel.endpoint(endpoint.uri());
                if (sender == null || sender.lifeStatus() != Endpoint.EndpointLifeStatus.BINDING) {
                    LOG.warn("[bindSender] operation {} cancelled, endpoint {} not found or has wrong lifeStatus",
                        bindOperation.id(), endpoint.uri());
                    toCancelOperation = true;
                    break;
                }

                List<Endpoint> receivers = channel.findReceiversToConnect(endpoint);
                if (receivers.isEmpty()) {
                    break;
                }
                receiverToConnect = channel.findReceiversToConnect(endpoint).get(0);

                withRetries(defaultRetryPolicy(), LOG, () -> channelStorage.insertEndpointConnection(
                    channelId, endpoint.uri().toString(), receiverToConnect.uri().toString(), null));
            } catch (Exception e) {
                LOG.error("[bindSender] operation {} failed", bindOperation.id(), e);
                sendError();
                return;
            }

            try {
                slotApiClient.connect(endpoint, receiverToConnect, Duration.ofSeconds(10));
            } catch (Exception e) {
                LOG.error("[bindSender] operation {} failed while connecting, sender={}, receiver={}",
                    bindOperation.id(), endpoint.uri(), receiverToConnect.uri(), e);
                sendError();
                return;
            }

            try (final var guard = lockManager.withLock(channelId)) {
                Channel channel = channelStorage.findChannel(channelId, ChannelStorage.ChannelLifeStatus.ALIVE, null);

                if (channel == null) {
                    LOG.warn("[bindSender] operation {} cancelled, channel {} not found",
                        bindOperation.id(), channelId);
                    toCancelOperation = true;
                    receiverToUndoConnection = receiverToConnect;
                    break;
                }

                Endpoint sender = channel.endpoint(endpoint.uri());
                if (sender == null || sender.lifeStatus() != Endpoint.EndpointLifeStatus.BINDING) {
                    LOG.warn("[bindSender] operation {} cancelled, endpoint {} has unexpected lifeStatus",
                        bindOperation.id(), endpoint.uri());
                    toCancelOperation = true;
                    receiverToUndoConnection = receiverToConnect;
                    break;
                }

                Connection ???connection = channel.connection(endpoint.uri(), receiverToConnect.uri());
                if (channel.connection(sender.uri(), receiverToConnect.uri()) == null
                    || connection.lifeStatus != CONNECTING)
                {
                    LOG.warn("[bindSender] operation {} undo connection,"
                             + " sender={}, receiver={}, unexpected connection lifeStatus",
                        bindOperation.id(), endpoint.uri(), receiverToConnect.uri());
                    receiverToUndoConnection = receiverToConnect;
                } else {
                    withRetries(defaultRetryPolicy(), LOG, () -> channelStorage.markEndpointConnectionAlive(
                        channelId, endpoint.uri().toString(), receiverToConnect.uri().toString(), null));
                }
            } catch (Exception e) {
                // TODO
            }

            if (toUndoConnection) {
                try {
                    slotApiClient.disconnect(receiverToConnect);
                } catch (Exception e) {
                    // TODO
                }
            }

        }

        if (receiverToUndoConnection != null) {
            try {
                slotApiClient.disconnect(receiverToUndoConnection);
            } catch (Exception e) {
                // TODO
            }
        }

        try (final var guard = lockManager.withLock(channelId)) {
            if (toCancelOperation) {
                bindOperation.setError(Status.CANCELLED);
                operationStorage.update(bindOperation);
            } else {
                bindOperation.setResponse(Any.pack(LCMS.BindRequest.getDefaultInstance()));
                operationStorage.update(bindOperation);
                channelStorage.markEndpointBound(endpoint.uri().toString(), null);
            }
        } catch (Exception e) {
            // TODO
        }
    }

    private void bindReceiver(Endpoint endpoint, Operation bindOperation) {
        final String channelId = endpoint.channelId();

        boolean toCancelOperation = false;
        boolean toUndoConnection = false;
        final Endpoint senderToConnect;
        try (final var guard = lockManager.withLock(channelId)) {
            Channel channel = channelStorage.findChannel(channelId, ChannelStorage.ChannelLifeStatus.ALIVE, null);

            if (channel == null) {
                LOG.warn("[bindReceiver] operation {} cancelled, channel {} not found",
                    bindOperation.id(), channelId);
                toCancelOperation = true;
                break;
            }

            Endpoint receiver = channel.endpoint(endpoint.uri());
            if (receiver == null || receiver.lifeStatus() != Endpoint.EndpointLifeStatus.BINDING) {
                LOG.warn("[bindReceiver] operation {} cancelled, endpoint {} not found or has wrong lifeStatus",
                    bindOperation.id(), endpoint.uri());
                toCancelOperation = true;
                break;
            }

            senderToConnect = channel.findSenderToConnect(endpoint);

            if (senderToConnect == null) {
                bindOperation.setResponse(Any.pack(LCMS.UnbindRequest.getDefaultInstance()));
                operationStorage.update(bindOperation);
                channelStorage.markEndpointBound(endpoint.uri().toString(), null);
                return;
            }

            withRetries(defaultRetryPolicy(), LOG, () -> channelStorage.insertEndpointConnection(
                channelId, senderToConnect.uri().toString(), endpoint.uri().toString(), null));
        } catch (Exception e) {
            // TODO
        }

        try {
            slotApiClient.connect(senderToConnect, endpoint, Duration.ofSeconds(10));
        } catch (Exception e) {
            // TODO
        }

        try (final var guard = lockManager.withLock(channelId)) {
            Channel channel = channelStorage.findChannel(channelId, ChannelStorage.ChannelLifeStatus.ALIVE, null);

            if (channel == null
                || channel.receiver(endpoint.uri()) == null || receiver.lifeStatus != BINDING)
            {
                toCancelOperation = true;
                toUndoConnection = true;
                break;
            }
            if (channel.connection(receiver.uri(), senderToConnect.uri()) == null ||
                connection.lifeStatus != CONNECTING)
            {
                toUndoConnection = true;
            } else {
                withRetries(defaultRetryPolicy(), LOG, () -> channelStorage.markEndpointConnectionAlive(
                    channelId, senderToConnect.uri().toString(), endpoint.uri().toString(), null));
            }
        } catch (Exception e) {
            // TODO
        }

    }

    private unbindSender(Channel channel, Endpoint endpoint) {
        /* for each connected receiver

            unbind receiver

        */

    }

    private unbindReceiver(Channel channel, Endpoint endpoint) {
        /* single edge */

        // set status disconnecting

        // call disconnect
    }

    private Endpoint resolveBoundUnconnectedReceiver() {
        return null;
    }

    @Nullable
    private Endpoint findEndpointToConnect(Endpoint endpoint) throws Exception {
        final String channelId = endpoint.channelId();
        try (final var guard = lockManager.withLock(channelId)) {
            final Channel channel = channelStorage.findChannel(channelId, ChannelStorage.ChannelLifeStatus.ALIVE, null);

            if (channel == null) {
                throw new CancellingStateException("channel " + channelId + " not found");
            }

            Endpoint actualStateEndpoint = channel.endpoint(endpoint.uri());
            if (actualStateEndpoint == null || actualStateEndpoint.lifeStatus() != Endpoint.EndpointLifeStatus.BINDING) {
                throw new CancellingStateException("endpoint " + endpoint.uri() + " has wrong lifeStatus");
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
                default -> throw new IllegalStateException("endpoint " + endpoint.uri() + " has unexpected direction");
            }

            if (endpointToConnect == null) {
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
            default -> throw new IllegalStateException("endpoint " + endpoint.uri() + " has unexpected direction");
        }

        try (final var guard = lockManager.withLock(channelId)) {
            Channel channel = channelStorage.findChannel(channelId, ChannelStorage.ChannelLifeStatus.ALIVE, null);

            if (channel == null) {
                throw new CancellingStateException("channel " + channelId + " not found");
            }

            Endpoint actualStateEndpoint = channel.endpoint(endpoint.uri());
            if (actualStateEndpoint == null || actualStateEndpoint.lifeStatus() != Endpoint.EndpointLifeStatus.BINDING) {
                throw new CancellingStateException("endpoint " + endpoint.uri() + " has wrong lifeStatus");
            }

            if (newConnection.lifeStatus != CONNECTING) {
                return false;
            }

            withRetries(defaultRetryPolicy(), LOG, () -> channelStorage.markEndpointConnectionAlive(
                channelId, sender.uri().toString(), receiver.uri().toString(), null));

            return true;
        }
    }

    private class CancellingStateException extends Exception {

        public CancellingStateException(String message) {
            super(message);
        }

        @Override
        public synchronized Throwable fillInStackTrace() {
            return this;
        }
    }

}
