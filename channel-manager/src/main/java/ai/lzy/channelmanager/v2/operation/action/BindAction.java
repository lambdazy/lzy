package ai.lzy.channelmanager.v2.operation.action;

import ai.lzy.channelmanager.db.ChannelManagerDataSource;
import ai.lzy.channelmanager.lock.GrainedLock;
import ai.lzy.channelmanager.v2.control.ChannelController;
import ai.lzy.channelmanager.v2.dao.ChannelDao;
import ai.lzy.channelmanager.v2.dao.ChannelOperationDao;
import ai.lzy.channelmanager.v2.exceptions.CancellingChannelGraphStateException;
import ai.lzy.channelmanager.v2.grpc.SlotConnectionManager;
import ai.lzy.channelmanager.v2.model.Channel;
import ai.lzy.channelmanager.v2.model.Connection;
import ai.lzy.channelmanager.v2.model.Endpoint;
import ai.lzy.channelmanager.v2.operation.ChannelOperationExecutor;
import ai.lzy.channelmanager.v2.operation.state.BindActionState;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.grpc.ProtoConverter;
import ai.lzy.v1.channel.v2.LCMS;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.slots.LSA;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Any;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;
import javax.annotation.Nullable;

import static ai.lzy.model.db.DbHelper.withRetries;

public class BindAction extends ChannelAction {

    private static final Logger LOG = LogManager.getLogger(BindAction.class);

    private BindActionState localState;
    private BindActionState state;

    public BindAction(String operationId, BindActionState state,
                      ObjectMapper objectMapper, ChannelOperationExecutor executor,
                      ChannelManagerDataSource storage, ChannelDao channelDao, OperationDao operationDao,
                      ChannelOperationDao channelOperationDao, ChannelController channelController,
                      SlotConnectionManager slotConnectionManager, GrainedLock lockManager)
    {
        super(operationId, objectMapper, executor, storage, channelDao, operationDao, channelOperationDao,
            channelController, slotConnectionManager, lockManager);
        this.state = state;
        this.localState = BindActionState.copyOf(state);
    }

    @Override
    public void run() {
        LOG.info("Async operation (operationId={}) resumed, channelId={}", operationId, state.channelId());
        operationStopped = false;

        try {

            final Endpoint bindingEndpoint = withRetries(LOG, () -> channelDao.findEndpoint(state.endpointUri(), null));

            if (bindingEndpoint == null) {
                String errorMessage = "Async operation (operationId=" + operationId + ") cancelled,"
                                      + " endpoint " + state.endpointUri()
                                      + " of channel " + state.channelId() + " not found";
                LOG.error(errorMessage);
                this.failOperation(Status.CANCELLED.withDescription(errorMessage));
                return;
            }

            while (true) {
                final Endpoint connectingEndpoint = resolveConnectingEndpoint(bindingEndpoint);
                if (operationStopped || connectingEndpoint == null) {
                    return;
                }

                final Connection potentialConnection = Connection.of(bindingEndpoint, connectingEndpoint);
                final var sender = potentialConnection.sender();
                final var receiver = potentialConnection.receiver();

                sendConnectOperation(sender, receiver);
                if (operationStopped) {
                    return;
                }

                final var connectSlotOperation = awaitConnectOperationDone(sender, receiver);
                if (operationStopped || connectSlotOperation == null) {
                    return;
                }

                if (!connectSlotOperation.hasResponse()) {
                    String errorMessage = "connectSlot operation with connectOperationId=" + state.connectOperationId()
                                          + " failed: " + connectSlotOperation.getError();
                    throw new RuntimeException(errorMessage);
                }

                saveConnection(bindingEndpoint, connectingEndpoint);
                if (operationStopped) {
                    return;
                }
            }

        } catch (CancellingChannelGraphStateException e) {
            String errorMessage = "Async operation (operationId=" + operationId + ") cancelled "
                                  + "due to the graph state: " + e.getMessage();
            LOG.error(errorMessage);
            this.failOperation(Status.CANCELLED.withDescription(errorMessage));
        } catch (Exception e) {
            String errorMessage = "Async operation (operationId=" + operationId + ") failed: " + e.getMessage();
            LOG.error(errorMessage);
            this.failOperation(Status.INTERNAL.withDescription(errorMessage));
        }

    }

    @Nullable
    private Endpoint resolveConnectingEndpoint(Endpoint bindingEndpoint) throws Exception {
        if (state.connectingEndpointUri() != null) {
            final Endpoint restoredEndpoint = withRetries(LOG, () ->
                channelDao.findEndpoint(state.connectingEndpointUri(), null));

            if (restoredEndpoint != null) {
                LOG.info("Async operation (operationId={}): restored connecting endpoint {}",
                    operationId, restoredEndpoint.getUri());
                return restoredEndpoint;
            }

            LOG.warn("Failed to restore connecting endpoint {}, not found", state.connectingEndpointUri());
        }

        localState.reset();
        withRetries(LOG, () -> channelOperationDao.update(operationId, toJson(localState), null));
        state = BindActionState.copyOf(localState);

        final Endpoint connectingEndpoint;
        try (final var guard = lockManager.withLock(state.channelId())) {
            final Channel channel = withRetries(LOG, () -> channelDao.findChannel(state.channelId(), null));
            if (channel == null) {
                String errorMessage = "Async operation (operationId=" + operationId + ") cancelled,"
                                      + " channel " + state.channelId() + " not found";
                LOG.error(errorMessage);
                this.failOperation(Status.CANCELLED.withDescription(errorMessage));
                return null;
            }

            connectingEndpoint = channelController.findEndpointToConnect(channel, bindingEndpoint);
            if (connectingEndpoint == null) {
                finishOperation();
                return null;
            }

            LOG.info("Async operation (operationId={}): found endpoint to connect {}",
                operationId, connectingEndpoint.getUri());
            localState = BindActionState.copyOf(state);
            localState.setConnectingEndpointUri(connectingEndpoint.getUri().toString());
            withRetries(LOG, () -> {
                try (var tx = TransactionHandle.create(storage)) {
                    channelOperationDao.update(operationId, toJson(localState), tx);
                    channelDao.insertConnection(state.channelId(),
                        Connection.of(bindingEndpoint, connectingEndpoint), tx);
                    tx.commit();
                }
            });
            state = BindActionState.copyOf(localState);

            return connectingEndpoint;
        }
    }

    private void saveConnection(Endpoint bindingEndpoint, Endpoint connectedEndpoint) throws Exception {
        final Connection connection = Connection.of(bindingEndpoint, connectedEndpoint);

        try (final var guard = lockManager.withLock(state.channelId())) {
            final Channel channel = withRetries(LOG, () -> channelDao.findChannel(state.channelId(), null));
            if (channel == null) {
                String errorMessage = "Async operation (operationId=" + operationId + ") cancelled,"
                                      + " channel " + state.channelId() + " not found";
                LOG.error(errorMessage);
                this.failOperation(Status.CANCELLED.withDescription(errorMessage));
                return;
            }

            boolean needToSave = channelController.checkChannelForSavingConnection(
                channel, bindingEndpoint, connectedEndpoint);

            if (needToSave) {
                localState.reset();
                try {
                    withRetries(LOG, () -> {
                        try (var tx = TransactionHandle.create(storage)) {
                            channelOperationDao.update(operationId, toJson(localState), tx);
                            channelDao.markConnectionAlive(
                                state.channelId(),
                                connection.sender().getUri().toString(),
                                connection.receiver().getUri().toString(),
                                tx);
                            tx.commit();
                        }
                    });
                    state = BindActionState.copyOf(localState);
                    LOG.info("Async operation (operationId={}): connection saved, "
                             + "bindingEndpoint={}, connectedEndpoint={}",
                        operationId, bindingEndpoint.getUri(), connectedEndpoint.getUri());
                } catch (Exception e) {
                    String errorMessage = "Failed to save alive connection"
                                          + ", bindingEndpoint=" + bindingEndpoint.getUri()
                                          + ", connectedEndpoint=" + connectedEndpoint.getUri()
                                          + ": " + e.getMessage();
                    throw new RuntimeException(errorMessage);
                }
            } else {
                LOG.info("Async operation (operationId={}): skip saving connection, disconnecting endpoints, "
                         + "sender={}, receiver={}", operationId,
                    connection.sender().getUri(), connection.receiver().getUri());
                state.reset();
                withRetries(LOG, () -> channelOperationDao.update(operationId, toJson(state), null));
            }
        }
    }

    private void finishOperation() throws Exception {
        try {
            withRetries(LOG, () -> {
                try (var tx = TransactionHandle.create(storage)) {
                    channelOperationDao.delete(operationId, tx);
                    operationDao.updateResponse(operationId,
                        Any.pack(LCMS.BindResponse.getDefaultInstance()).toByteArray(), tx);
                    channelDao.markEndpointBound(state.endpointUri(), tx);
                    tx.commit();
                }
            });
            LOG.info("Async operation (operationId={}) finished", operationId);
            operationStopped = true;
        } catch (SQLException e) {
            LOG.error("Async operation (operationId={}): failed to finish: {}. Schedule restart action",
                operationId, e.getMessage());
            scheduleRestart();
        }
    }


    private void sendConnectOperation(Endpoint sender, Endpoint receiver) throws Exception {
        if (state.connectOperationId() == null) {
            if (localState.connectOperationId() == null) {
                LOG.info("Async operation (operationId={}): sending connectSlot request", operationId);

                final var request = LSA.ConnectSlotRequest.newBuilder()
                    .setFrom(ProtoConverter.toProto(receiver.getSlot()))
                    .setTo(ProtoConverter.toProto(sender.getSlot()))
                    .build();

                final var slotApi = slotConnectionManager.getConnection(receiver.getUri()).slotApiBlockingStub();
                try {
                    final LongRunning.Operation connectSlotOperation = slotApi.connectSlot(request);
                    localState = BindActionState.copyOf(state);
                    localState.setConnectOperationId(connectSlotOperation.getId());
                } catch (StatusRuntimeException e) {
                    LOG.error("Async operation (operationId={}): "
                              + "connectSlot failed with code {}: {}. Schedule restart action",
                        operationId, e.getStatus().getCode(), e.getStatus().getDescription());
                    scheduleRestart();
                    return;
                }

                LOG.info("Async operation (operationId={}): sent connectSlot request, connectOperationId={}",
                    operationId, localState.connectOperationId());
            }

            try {
                withRetries(LOG, () -> channelOperationDao.update(operationId, toJson(localState), null));
                state = BindActionState.copyOf(localState);
            } catch (SQLException e) {
                LOG.error("Async operation (operationId={}): Cannot save state with connectOperationId={}. "
                          + "Schedule restart action", operationId, localState.connectOperationId());
                scheduleRestart();
            }
        }
    }

    @Nullable
    private LongRunning.Operation awaitConnectOperationDone(Endpoint sender, Endpoint receiver) {
        LOG.info("Async operation (operationId={}): check connectSlot status, connectOperationId={}",
            operationId, state.connectOperationId());

        final var operationApi = slotConnectionManager.getConnection(receiver.getUri()).operationApiBlockingStub();
        final LongRunning.Operation operation;
        try {
            operation = operationApi.get(LongRunning.GetOperationRequest.newBuilder()
                .setOperationId(state.connectOperationId())
                .build());
        } catch (StatusRuntimeException e) {
            LOG.error("Async operation (operationId={}): check connectSlot status with connectOperationId={}"
                      + " failed with code {}: {}. Schedule restart action", operationId, state.connectOperationId(),
                e.getStatus().getCode(), e.getStatus().getDescription());
            scheduleRestart();
            return null;
        }

        if (!operation.getDone()) {
            LOG.info("Async operation (operationId={}): connectSlot operation with connectOperationId={}"
                     + " not completed yet. Schedule restart action", operationId, state.connectOperationId());
            scheduleRestart();
            return null;
        }

        LOG.info("Async operation (operationId={}): got response from connectSlot status, connectOperationId={}",
            operationId, state.connectOperationId());
        return operation;
    }

}