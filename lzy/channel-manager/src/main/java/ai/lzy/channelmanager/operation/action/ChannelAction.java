package ai.lzy.channelmanager.operation.action;

import ai.lzy.channelmanager.control.ChannelController;
import ai.lzy.channelmanager.dao.ChannelDao;
import ai.lzy.channelmanager.dao.ChannelManagerDataSource;
import ai.lzy.channelmanager.dao.ChannelOperationDao;
import ai.lzy.channelmanager.exceptions.ChannelGraphStateException;
import ai.lzy.channelmanager.grpc.SlotConnectionManager;
import ai.lzy.channelmanager.lock.GrainedLock;
import ai.lzy.channelmanager.model.Connection;
import ai.lzy.channelmanager.model.Endpoint;
import ai.lzy.channelmanager.model.channel.Channel;
import ai.lzy.channelmanager.operation.ChannelOperationExecutor;
import ai.lzy.channelmanager.test.InjectedFailures;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.NotFoundException;
import ai.lzy.model.grpc.ProtoConverter;
import ai.lzy.v1.slots.LSA;
import ai.lzy.v1.workflow.LWFPS;
import ai.lzy.v1.workflow.LzyWorkflowPrivateServiceGrpc;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.util.grpc.ProtoConverter.toProto;

public abstract class ChannelAction implements Runnable {

    private static final Logger LOG = LogManager.getLogger(ChannelAction.class);

    private final ObjectMapper objectMapper;
    private final ChannelOperationExecutor executor;
    private final LzyWorkflowPrivateServiceGrpc.LzyWorkflowPrivateServiceBlockingStub workflowPrivateApi;

    protected final String operationId;
    protected final Instant deadline;
    protected final ChannelManagerDataSource storage;
    protected final ChannelDao channelDao;
    protected final OperationDao operationDao;
    protected final ChannelOperationDao channelOperationDao;
    protected final ChannelController channelController;
    protected final SlotConnectionManager slotConnectionManager;
    protected final GrainedLock lockManager;

    protected boolean operationStopped = false;

    protected ChannelAction(String operationId, Instant deadline, ObjectMapper objectMapper,
                            ChannelOperationExecutor executor, ChannelManagerDataSource storage,
                            ChannelDao channelDao, OperationDao operationDao,
                            ChannelOperationDao channelOperationDao, ChannelController channelController,
                            SlotConnectionManager slotConnectionManager, GrainedLock lockManager,
                            LzyWorkflowPrivateServiceGrpc.LzyWorkflowPrivateServiceBlockingStub workflowPrivateApi)
    {
        this.operationId = operationId;
        this.deadline = deadline;
        this.objectMapper = objectMapper;
        this.workflowPrivateApi = workflowPrivateApi;
        this.executor = executor;
        this.storage = storage;
        this.channelDao = channelDao;
        this.operationDao = operationDao;
        this.channelOperationDao = channelOperationDao;
        this.channelController = channelController;
        this.slotConnectionManager = slotConnectionManager;
        this.lockManager = lockManager;
    }

    protected void scheduleRestart() {
        scheduleRestart(Duration.ofMillis(300));
    }

    protected void scheduleRestart(Duration delay) {
        operationStopped = true;
        executor.schedule(this, delay.toMillis(), TimeUnit.MILLISECONDS);
    }

    protected void failOperation(String executionId, Status status) {
        operationStopped = true;
        try {
            withRetries(LOG, () -> {
                try (var tx = TransactionHandle.create(storage)) {
                    channelOperationDao.fail(operationId, status.getDescription(), tx);
                    operationDao.fail(operationId, toProto(status), tx);
                    tx.commit();
                }
            });
        } catch (NotFoundException ex) {
            LOG.error("Cannot fail operation {} with reason {}: operation not found",
                operationId, status.getDescription());
        } catch (Exception ex) {
            LOG.error("Cannot fail operation {} with reason {}: {}",
                operationId, status.getDescription(), ex.getMessage());
        }

        try {
            //noinspection ResultOfMethodCallIgnored
            workflowPrivateApi.abortExecution(LWFPS.AbortExecutionRequest.newBuilder()
                .setExecutionId(executionId)
                .setReason(status.getDescription())
                .build());
            LOG.info("Sent request abortExecution {} about failed operation {}", executionId, operationId);
        } catch (Exception e) {
            LOG.error("Cannot send request abortExecution {} about failed operation {}, got exception: {}",
                executionId, operationId, e.getMessage());
        }
    }

    protected String toJson(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    protected void unbindSender(Endpoint sender, boolean onError) throws ChannelGraphStateException {
        LOG.info("Async operation (operationId={}): unbind sender {}", operationId, sender.getUri());

        final String channelId = sender.getChannelId();
        while (true) {
            final Endpoint receiverToUnbind;
            try (final var guard = lockManager.withLock(sender.getChannelId())) {
                final Channel channel;
                try {
                    channel = withRetries(LOG, () -> channelDao.findChannel(sender.getChannelId(), null));
                } catch (Exception e) {
                    String errorMessage = "Failed to find channel" + sender.getChannelId()
                        + " got exception: " + e.getMessage();
                    throw new RuntimeException(errorMessage);
                }
                if (channel == null) {
                    LOG.info("Async operation (operationId=" + operationId + ") skipped,"
                        + " channel " + sender.getChannelId() + " not found");
                    return;
                }

                receiverToUnbind = channelController.findReceiverToUnbind(channel, sender);
                if (receiverToUnbind == null) {
                    LOG.info("Async operation (operationId={}): bound receivers not found, unbindingSender={}",
                        operationId, sender.getUri());
                    break;
                } else {
                    try {
                        withRetries(LOG, () ->
                            channelDao.markEndpointUnbinding(receiverToUnbind.getUri().toString(), null));
                    } catch (Exception e) {
                        String errorMessage = "Failed to mark receiver " + receiverToUnbind.getUri() + " unbinding,"
                            + " got exception: " + e.getMessage();
                        throw new RuntimeException(errorMessage);
                    }
                }
            }

            LOG.info("Async operation (operationId={}): found bound receiver {}, need force unbind, unbindingSender={}",
                operationId, receiverToUnbind.getUri(), sender.getUri());

            InjectedFailures.fail6();

            this.unbindReceiver(receiverToUnbind, onError);
            if (operationStopped) {
                return;
            }

            InjectedFailures.fail7();
        }

        disconnect(sender);
        if (operationStopped) {
            return;
        }

        // TODO (lindvv): maybe should suspend, so shouldn't destroy, fix after slots refactoring
        destroySlot(sender, onError);
        if (operationStopped) {
            return;
        }

        InjectedFailures.fail8();

        try (final var guard = lockManager.withLock(channelId)) {
            withRetries(LOG, () -> channelDao.removeEndpoint(sender.getUri().toString(), null));
            LOG.info("Async operation (operationId={}): sender unbound,"
                + " unbindingSender={}", operationId, sender.getUri());
        } catch (Exception e) {
            String errorMessage = "Failed to remove sender " + sender.getUri() + ", got exception: " + e.getMessage();
            throw new RuntimeException(errorMessage);
        }
    }

    protected void unbindReceiver(Endpoint receiver, boolean onError) throws ChannelGraphStateException {
        LOG.info("Async operation (operationId={}): unbind receiver {}", operationId, receiver.getUri());

        final String channelId = receiver.getChannelId();

        final Connection connectionToBreak;
        try (final var guard = lockManager.withLock(channelId)) {
            final Channel channel;
            try {
                channel = withRetries(LOG, () -> channelDao.findChannel(receiver.getChannelId(), null));
            } catch (Exception e) {
                String errorMessage = "Failed to find channel" + receiver.getChannelId()
                    + " got exception: " + e.getMessage();
                throw new RuntimeException(errorMessage);
            }
            if (channel == null) {
                LOG.info("Async operation (operationId=" + operationId + ") skipped,"
                    + " channel " + receiver.getChannelId() + " not found");
                return;
            }

            connectionToBreak = channelController.findConnectionToBreak(channel, receiver);
            if (connectionToBreak != null) {
                final Endpoint sender = connectionToBreak.sender();
                try {
                    withRetries(LOG, () -> channelDao.markConnectionDisconnecting(channelId,
                        sender.getUri().toString(), receiver.getUri().toString(), null));
                    LOG.info("Async operation (operationId={}): marked connection disconnecting,"
                        + " unbindingReceiver={}, sender={}", operationId, receiver.getUri(), sender.getUri());
                } catch (Exception e) {
                    throw new RuntimeException("Failed to mark connection disconnecting in storage", e);
                }
            }
        }

        InjectedFailures.fail4();

        disconnect(receiver);
        if (operationStopped) {
            return;
        }

        // TODO (lindvv): maybe should suspend, so shouldn't destroy, fix after slots refactoring
        destroySlot(receiver, onError);
        if (operationStopped) {
            return;
        }

        InjectedFailures.fail5();

        try (final var guard = lockManager.withLock(channelId)) {
            if (connectionToBreak != null) {
                withRetries(LOG, () -> channelDao.removeConnection(channelId,
                    connectionToBreak.sender().getUri().toString(),
                    connectionToBreak.receiver().getUri().toString(),
                    null));
                LOG.info("Async operation (operationId={}): connection removed, sender={}, unbindingReceiver={}",
                    operationId, connectionToBreak.sender().getUri(), receiver.getUri());
            }
            withRetries(LOG, () -> channelDao.removeEndpoint(receiver.getUri().toString(), null));
            LOG.info("Async operation (operationId={}): receiver removed,"
                + " unbindingReceiver={}", operationId, receiver.getUri());
        } catch (Exception e) {
            String errorMessage = "Failed to remove receiver " + receiver.getUri() + " with connection,"
                + " got exception: " + e.getMessage();
            throw new RuntimeException(errorMessage);
        }
    }

    protected void disconnect(Endpoint endpoint) {
        final var slotApi = slotConnectionManager.getConnection(endpoint.getUri()).slotApiBlockingStub();
        try {
            final var request = LSA.DisconnectSlotRequest.newBuilder()
                .setSlotInstance(ProtoConverter.toProto(endpoint.getSlot()))
                .build();

            slotApi.disconnectSlot(request);
        } catch (StatusRuntimeException e) {
            if (Status.NOT_FOUND.getCode().equals(e.getStatus().getCode()) ||
                Status.UNAVAILABLE.getCode().equals(e.getStatus().getCode()))
            {
                LOG.info("Async operation (operationId={}): disconnectSlot request failed, slot not found. "
                    + "Continue action", operationId);
                return;
            } else {
                LOG.error("Async operation (operationId={}): disconnectSlot request failed"
                        + " with code {}: {}. Schedule restart action",
                    operationId, e.getStatus().getCode(), e.getStatus().getDescription());
                scheduleRestart();
                return;
            }
        }
        LOG.info("Async operation (operationId={}): sent disconnectSlot request", operationId);
    }

    protected void destroySlot(Endpoint endpoint, boolean onError) {
        final var slotApi = slotConnectionManager.getConnection(endpoint.getUri()).slotApiBlockingStub();
        try {
            final var request = LSA.DestroySlotRequest.newBuilder()
                .setSlotInstance(ProtoConverter.toProto(endpoint.getSlot()))
                .setReason(onError ? "Some error occurs" : "")
                .build();

            slotApi.destroySlot(request);
        } catch (StatusRuntimeException e) {
            if (Status.NOT_FOUND.getCode().equals(e.getStatus().getCode()) ||
                Status.UNAVAILABLE.getCode().equals(e.getStatus().getCode()))
            {
                LOG.info("Async operation (operationId={}): destroySlot request failed, slot not found. "
                    + "Continue action", operationId);
                return;
            } else {
                LOG.error("Async operation (operationId={}): destroySlot request failed"
                        + " with code {}: {}. Schedule restart action",
                    operationId, e.getStatus().getCode(), e.getStatus().getDescription());
                scheduleRestart();
                return;
            }
        }
        LOG.info("Async operation (operationId={}): sent destroySlot request", operationId);
    }

}
