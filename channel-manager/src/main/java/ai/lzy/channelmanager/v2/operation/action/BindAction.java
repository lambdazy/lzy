package ai.lzy.channelmanager.v2.operation.action;

import ai.lzy.channelmanager.db.ChannelManagerDataSource;
import ai.lzy.channelmanager.lock.GrainedLock;
import ai.lzy.channelmanager.v2.control.ChannelController;
import ai.lzy.channelmanager.v2.db.ChannelDao;
import ai.lzy.channelmanager.v2.exceptions.CancellingChannelGraphStateException;
import ai.lzy.channelmanager.v2.model.Connection;
import ai.lzy.channelmanager.v2.model.Endpoint;
import ai.lzy.channelmanager.v2.operation.ChannelOperationDao;
import ai.lzy.channelmanager.v2.operation.state.BindActionState;
import ai.lzy.channelmanager.v2.slot.SlotApiClient;
import ai.lzy.channelmanager.v2.slot.SlotConnectionManager;
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

import static ai.lzy.model.db.DbHelper.withRetries;

public class BindAction extends ChannelAction {

    private static final Logger LOG = LogManager.getLogger(BindAction.class);

    private BindActionState localState;
    private BindActionState state;

    public BindAction(ObjectMapper objectMapper, String operationId, ChannelManagerDataSource storage,
                      ChannelDao channelDao, OperationDao operationDao, ChannelOperationDao channelOperationDao,
                      SlotApiClient slotApiClient, ChannelController channelController, GrainedLock lockManager,
                      SlotConnectionManager slotConnectionManager, BindActionState state)
    {
        super(objectMapper, operationId, storage, channelDao, operationDao, channelOperationDao, slotApiClient,
            channelController, slotConnectionManager, lockManager);
        this.state = state;
        this.localState = BindActionState.copyOf(state);
    }

    @Override
    public void run() {
        LOG.info("Async operation (operationId={}) resumed, channelId={}", operationId, state.channelId());

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
                final Endpoint restoredEndpoint;
                if (state.connectingEndpointUri() == null) {
                    restoredEndpoint = null;
                } else {
                    restoredEndpoint = withRetries(LOG, () ->
                        channelDao.findEndpoint(state.connectingEndpointUri(), null));

                    if (restoredEndpoint == null) {
                        LOG.warn("Failed to restore connecting endpoint {}, not found", state.connectingEndpointUri());
                    }
                }

                final Endpoint connectingEndpoint;
                if (restoredEndpoint != null) {
                    connectingEndpoint = restoredEndpoint;
                    LOG.info("Async operation (operationId={}): restored connecting endpoint {}",
                        operationId, connectingEndpoint.getUri());
                } else {
                    localState.reset();
                    withRetries(LOG, () -> channelOperationDao.update(operationId, toJson(localState), null));
                    state = BindActionState.copyOf(localState);

                    try (final var guard = lockManager.withLock(state.channelId())) {
                        connectingEndpoint = channelController.findEndpointToConnect(bindingEndpoint);
                        if (connectingEndpoint == null) {
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
                            } catch (Exception e) {
                                LOG.error("Async operation (operationId={}): failed to finish: {}. Restart action",
                                    operationId, e.getMessage());
                                scheduleRestart();
                            }
                            return;
                        }

                        LOG.info("Async operation (operationId={}): found connecting endpoint {}",
                            operationId, connectingEndpoint.getUri());
                        localState = BindActionState.copyOf(state);
                        localState.setConnectingEndpointUri(connectingEndpoint.getUri().toString());
                        try {
                            withRetries(LOG, () -> {
                                try (var tx = TransactionHandle.create(storage)) {
                                    channelOperationDao.update(operationId, toJson(localState), tx);
                                    channelDao.insertConnection(state.channelId(),
                                        Connection.of(bindingEndpoint, connectingEndpoint), tx);
                                    tx.commit();
                                }
                            });
                            state = BindActionState.copyOf(localState);
                        } catch (Exception e) {
                            String errorMessage = "Failed to insert connection "
                                                  + "(endpointToConnect= " + state.endpointUri() + ")"
                                                  + "got exception: " + e.getMessage();
                            throw new RuntimeException(errorMessage);
                        }
                    }
                }

                final Connection potentialConnection = Connection.of(bindingEndpoint, connectingEndpoint);
                final var sender = potentialConnection.sender();
                final var receiver = potentialConnection.receiver();

                if (state.connectOperationId() == null) {
                    if (localState.connectOperationId() == null) {
                        final var request = LSA.ConnectSlotRequest.newBuilder()
                            .setFrom(ProtoConverter.toProto(receiver.getSlot()))
                            .setTo(ProtoConverter.toProto(sender.getSlot()))
                            .build();

                        final var slotApi =
                            slotConnectionManager.getConnection(receiver.getUri()).slotApiBlockingStub();
                        try {
                            final LongRunning.Operation connectSlotOperation = slotApi.connectSlot(request);
                            localState = BindActionState.copyOf(state);
                            localState.setConnectOperationId(connectSlotOperation.getId());
                        } catch (StatusRuntimeException e) {
                            LOG.error("Async operation (operationId={}): "
                                      + "connectSlot failed with code {}: {}. Restart action",
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
                    } catch (Exception e) {
                        LOG.error("Async operation (operationId={}): Cannot save state with connectOperationId={}. "
                                   + "Restart action", operationId, localState.connectOperationId());
                        scheduleRestart();
                        return;
                    }
                }

                LOG.info("Async operation (operationId={}): check connectSlot status, connectOperationId={}",
                    operationId, state.connectOperationId());

                final var operationApi = slotConnectionManager.getConnection(receiver.getUri()).operationApiBlockingStub();
                final LongRunning.Operation connectSlotOperation;
                try {
                    connectSlotOperation = operationApi.get(LongRunning.GetOperationRequest.newBuilder()
                        .setOperationId(state.connectOperationId())
                        .build());
                } catch (StatusRuntimeException e) {
                    LOG.error("Async operation (operationId={}): check connectSlot status with connectOperationId={}"
                              + " failed with code {}: {}. Restart action", operationId, state.connectOperationId(),
                        e.getStatus().getCode(), e.getStatus().getDescription());
                    scheduleRestart();
                    return;
                }

                if (!connectSlotOperation.getDone()) {
                    LOG.info("Async operation (operationId={}): connectSlot operation with connectOperationId={}"
                              + " not completed yet. Restart action", operationId, state.connectOperationId());
                    scheduleRestart();
                    return;
                }

                if (!connectSlotOperation.hasResponse()) {
                    String errorMessage = "connectSlot operation with connectOperationId=" + state.connectOperationId()
                                          + " failed: " + connectSlotOperation.getError();
                    throw new RuntimeException(errorMessage);
                }

                try (final var guard = lockManager.withLock(state.channelId())) {
                    boolean needToSave = channelController.checkForSavingConnection(bindingEndpoint, connectingEndpoint);
                    if (needToSave) {
                        localState.reset();
                        try {
                            withRetries(LOG, () -> {
                                try (var tx = TransactionHandle.create(storage)) {
                                    channelOperationDao.update(operationId, toJson(localState), tx);
                                    channelDao.markConnectionAlive(
                                        state.channelId(),
                                        potentialConnection.sender().getUri().toString(),
                                        potentialConnection.receiver().getUri().toString(),
                                        tx);
                                    tx.commit();
                                }
                            });
                            state = BindActionState.copyOf(localState);
                            LOG.info("Async operation (operationId={}): connection saved, "
                                     + "bindingEndpoint={}, connectingEndpoint={}",
                                operationId, bindingEndpoint.getUri(), connectingEndpoint.getUri());
                        } catch (Exception e) {
                            throw new RuntimeException("Failed to save alive connection in storage", e);
                        }
                    } else {
                        LOG.info("Async operation (operationId={}): skip saving connection, disconnecting endpoints, "
                                 + "sender={}, receiver={}", operationId,
                            potentialConnection.sender().getUri(), potentialConnection.receiver().getUri());
                        slotApiClient.disconnect(potentialConnection.receiver());
                        state.reset();
                        withRetries(LOG, () -> channelOperationDao.update(operationId, toJson(state), null));
                    }
                } catch (CancellingChannelGraphStateException e) {
                    LOG.info("Async operation (operationId={}): saving connection failed, disconnecting endpoints, "
                             + " sender={}, receiver={}", operationId,
                        potentialConnection.sender().getUri(), potentialConnection.receiver().getUri());
                    slotApiClient.disconnect(potentialConnection.receiver());
                    state.reset();
                    withRetries(LOG, () -> channelOperationDao.update(operationId, toJson(state), null));
                    throw e;
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

}
