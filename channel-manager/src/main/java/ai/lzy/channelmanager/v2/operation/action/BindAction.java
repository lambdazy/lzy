package ai.lzy.channelmanager.v2.operation.action;

import ai.lzy.channelmanager.lock.GrainedLock;
import ai.lzy.channelmanager.v2.control.ChannelController;
import ai.lzy.channelmanager.v2.db.ChannelDao;
import ai.lzy.channelmanager.v2.exceptions.CancellingChannelGraphStateException;
import ai.lzy.channelmanager.v2.model.Channel;
import ai.lzy.channelmanager.v2.model.Connection;
import ai.lzy.channelmanager.v2.model.Endpoint;
import ai.lzy.channelmanager.v2.operation.ChannelOperationDao;
import ai.lzy.channelmanager.v2.operation.state.BindActionState;
import ai.lzy.channelmanager.v2.slot.SlotApiClient;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.v1.channel.v2.LCMS;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.protobuf.Any;
import io.grpc.Status;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import javax.annotation.Nullable;

import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.util.grpc.ProtoConverter.toProto;

public class BindAction implements ChannelAction {

    private static final Logger LOG = LogManager.getLogger(BindAction.class);

    private final String operationId;
    private final BindActionState state;
    private final ChannelDao channelDao;
    private final OperationDao operationDao;
    private final GrainedLock lockManager;
    private final SlotApiClient slotApiClient;
    private final ChannelOperationDao channelOperationDao;
    private final ChannelController channelController;

    public BindAction(BindActionState state, ChannelDao channelDao, OperationDao operationDao,
                      ChannelOperationDao channelOperationDao, ChannelController channelController)
    {
        this.state = state;
        this.channelDao = channelDao;
        this.operationDao = operationDao;
        this.channelOperationDao = channelOperationDao;
        this.channelController = channelController;
    }

    @Override
    public void run() {

        try {
            final Endpoint endpoint = withRetries(LOG, () -> channelDao.findEndpoint(state.endpointUri(), null));

            if (endpoint == null) {
                cancelOperation();
                return;
            }

            while (true) {
                String connectingEndpointUri = state.connectingEndpointUri();

                final Endpoint endpointToConnect;
                if (connectingEndpointUri == null) {
                    try (final var guard = lockManager.withLock(state.channelId())) {
                        endpointToConnect = findEndpointToConnect(endpoint);
                        if (endpointToConnect == null) {
                            withRetries(LOG, () -> {
                                try (var tx = TransactionHandle.create(storage)) {
                                    channelOperationDao.delete(operationId, tx);
                                    final var response = Any.pack(LCMS.BindResponse.getDefaultInstance()).toByteArray();
                                    operationDao.updateResponse(operationId, response, tx);
                                    channelDao.markEndpointBound(endpoint.getUri().toString(), null);
                                } catch (Exception e) {
                                    throw new RuntimeException("Failed to mark endpoint bound in storage");
                                }
                            });
                            return;
                        }
                    }
                } else {
                    endpointToConnect = withRetries(LOG, () -> channelDao.findEndpoint(connectingEndpointUri, null));
                    if (endpointToConnect == null) {
                        state.reset();
                        withRetries(LOG, () -> channelOperationDao.update(operationId, toJson(state), null));
                        continue;
                    }
                }

                final Connection potentialConnection = Connection.of(endpoint, endpointToConnect);

                try {
                    String connectOpId = state.connectOperationId();
                    if (connectOpId == null) {
                        connectOpId = slotApiClient.connectStart(
                            potentialConnection.sender(),
                            potentialConnection.receiver());

                        state.setConnectOperationId(connectOpId);
                        withRetries(LOG, () -> channelOperationDao.update(operationId, toJson(state), null));
                    }

                    slotApiClient.connectFinish(potentialConnection.sender(), potentialConnection.receiver(),
                        Duration.ofSeconds(10), connectOpId);

                } catch (Exception e) {
                    state.reset();
                    withRetries(LOG, () -> channelOperationDao.update(operationId, toJson(state), null));
                    // remove connection
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





        } catch (Exception e) {
            String errorMessage = operationDescription + " async operation " + operation.id()
                                  + " failed: " + e.getMessage();
            LOG.error(errorMessage);
            operationDao.failOperation(operation.id(),
                toProto(Status.INTERNAL.withDescription(errorMessage)), LOG);
            // TODO
        }



        try {
            LOG.info(operationDescription + " responded, async operation started, operationId={}", operation.id());

            final Endpoint endpoint = channelDao.findEndpoint(slotUri, null);


            channelController.bind(endpoint);

            try {
                withRetries(LOG, () -> operationDao.updateResponse(operation.id(),
                    Any.pack(LCMS.BindResponse.getDefaultInstance()).toByteArray(), null));
            } catch (Exception e) {
                LOG.error("Cannot update operation", e);
                return;
            }

            LOG.info(operationDescription + " responded, async operation finished, operationId={}", operation.id());
        } catch (CancellingChannelGraphStateException e) {
            String errorMessage = operationDescription + " async operation " + operation.id()
                                  + " cancelled according to the graph state: " + e.getMessage();
            LOG.error(errorMessage);
            operationDao.failOperation(operation.id(),
                toProto(Status.CANCELLED.withDescription(errorMessage)), LOG);
        } catch (Exception e) {
            String errorMessage = operationDescription + " async operation " + operation.id()
                                  + " failed: " + e.getMessage();
            LOG.error(errorMessage);
            operationDao.failOperation(operation.id(),
                toProto(Status.INTERNAL.withDescription(errorMessage)), LOG);
            // TODO
        }

    }

    @Nullable
    private Endpoint findEndpointToConnect(Endpoint bindingEndpoint) throws CancellingChannelGraphStateException {
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

        try {
            state.setConnectingEndpointUri(endpointToConnect.getUri().toString());
            withRetries(LOG, () -> {
                try (var tx = TransactionHandle.create(storage)) {
                    channelOperationDao.update(operationId, toJson(state), tx);
                    channelDao.insertConnection(state.channelId(),
                        Connection.of(bindingEndpoint, endpointToConnect), tx);
                }
            });
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
            LOG.warn("[saveConnection] skipped, unexpected connection status {}, "
                     + "bindingEndpoint={}, connectedEndpoint={}",
                connectionStatus, bindingEndpoint.getUri(), connectedEndpoint.getUri());
            return false;
        }

        try {
            state.reset();
            withRetries(LOG, () -> {
                try (var tx = TransactionHandle.create(storage)) {
                    channelOperationDao.update(operationId, toJson(state), tx);
                    channelDao.markConnectionAlive(channelId,
                        sender.getUri().toString(), receiver.getUri().toString(), tx);
                }
            });
        } catch (Exception e) {
            throw new RuntimeException("Failed to save alive connection in storage", e);
        }

        LOG.debug("[saveConnection] done, bindingEndpoint={}, connectedEndpoint={}",
            bindingEndpoint.getUri().toString(), connectedEndpoint.getUri().toString());

        return true;
    }

    private void cancelOperation() {

    }

    protected final String toJson(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
