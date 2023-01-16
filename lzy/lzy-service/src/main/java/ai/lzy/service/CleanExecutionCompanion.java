package ai.lzy.service;

import ai.lzy.longrunning.Operation;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.service.data.dao.ExecutionDao;
import ai.lzy.service.data.dao.GraphDao;
import ai.lzy.service.data.dao.PortalDescription;
import ai.lzy.service.data.storage.LzyServiceStorage;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.v1.AllocatorGrpc;
import ai.lzy.v1.VmAllocatorApi;
import ai.lzy.v1.channel.LCMPS;
import ai.lzy.v1.channel.LzyChannelManagerPrivateGrpc;
import ai.lzy.v1.graph.GraphExecutorApi;
import ai.lzy.v1.graph.GraphExecutorGrpc;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import ai.lzy.v1.portal.LzyPortalApi;
import ai.lzy.v1.portal.LzyPortalGrpc;
import com.google.common.net.HostAndPort;
import com.google.protobuf.Any;
import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.annotation.Nullable;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;

import static ai.lzy.longrunning.OperationUtils.awaitOperationDone;
import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.service.LzyService.APP;
import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;
import static ai.lzy.util.grpc.ProtoConverter.toProto;

@Singleton
public class CleanExecutionCompanion {
    private static final Logger LOG = LogManager.getLogger(CleanExecutionCompanion.class);

    private final LzyServiceStorage storage;
    private final ExecutionDao executionDao;
    private final GraphDao graphDao;
    private final OperationDao operationDao;

    private final RenewableJwt internalUserCredentials;

    private final ManagedChannel channelManagerChannel;
    private final Map<String, ManagedChannel> portalChannels;
    private final LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateBlockingStub channelManagerClient;
    private final GraphExecutorGrpc.GraphExecutorBlockingStub graphExecutorClient;
    private final AllocatorGrpc.AllocatorBlockingStub allocatorClient;

    public CleanExecutionCompanion(LzyServiceStorage storage, ExecutionDao executionDao, GraphDao graphDao,
                                   @Named("LzyServiceOperationDao") OperationDao operationDao,
                                   @Named("LzyServiceIamToken") RenewableJwt internalUserCredentials,
                                   @Named("ChannelManagerServiceChannel") ManagedChannel channelManagerChannel,
                                   @Named("PortalChannels") Map<String, ManagedChannel> portalChannels,
                                   @Named("GraphExecutorServiceChannel") ManagedChannel graphExecutorChannel,
                                   @Named("AllocatorServiceChannel") ManagedChannel allocatorChannel)
    {
        this.storage = storage;
        this.executionDao = executionDao;
        this.graphDao = graphDao;
        this.operationDao = operationDao;

        this.internalUserCredentials = internalUserCredentials;

        this.portalChannels = portalChannels;
        this.channelManagerChannel = channelManagerChannel;
        this.channelManagerClient = newBlockingClient(
            LzyChannelManagerPrivateGrpc.newBlockingStub(channelManagerChannel), APP,
            () -> internalUserCredentials.get().token());
        this.graphExecutorClient = newBlockingClient(
            GraphExecutorGrpc.newBlockingStub(graphExecutorChannel), APP, () -> internalUserCredentials.get().token());
        this.allocatorClient = newBlockingClient(
            AllocatorGrpc.newBlockingStub(allocatorChannel), APP, () -> internalUserCredentials.get().token());
    }

    public boolean markExecutionAsBroken(String userId, String executionId, Status reason) {
        LOG.info("Attempt to mark execution as broken by reason: { executionId: {}, reason: {} }", executionId, reason);

        try {
            withRetries(LOG, () -> {
                try (var tx = TransactionHandle.create(storage)) {
                    executionDao.setErrorExecutionStatus(executionId, tx);
                    executionDao.updateFinishData(userId, executionId, reason, tx);

                    tx.commit();
                }
            });
        } catch (Exception e) {
            LOG.warn("Cannot mark execution as broken: { executionId: {}, error: {} }", executionId,
                e.getMessage());
            return false;
        }

        return true;
    }

    public void completeExecution(String executionId, Operation completeOperation) {
        LOG.info("Attempt to complete execution: { executionId: {} }", executionId);

        stopGraphs(executionId);

        var portalDesc = getPortalDescription(executionId);

        if (portalDesc != null && portalDesc.vmAddress() != null) {
            var shutdownPortalOp = shutdownPortal(executionId, portalDesc.vmAddress());
            if (shutdownPortalOp != null) {
                var grpcChannel = portalChannels.computeIfAbsent(executionId, exId ->
                    newGrpcChannel(portalDesc.vmAddress(), LzyPortalGrpc.SERVICE_NAME));
                var portalOpsClient = newBlockingClient(
                    LongRunningServiceGrpc.newBlockingStub(grpcChannel), APP,
                    () -> internalUserCredentials.get().token());

                // it may be long-running process to finish stdout/err portal slots
                shutdownPortalOp = awaitOperationDone(portalOpsClient, shutdownPortalOp.getId(), Duration.ofMinutes(5));

                if (!shutdownPortalOp.getDone()) {
                    LOG.warn("Cannot wait portal of execution shutdown: { executionId: {} }", executionId);
                    stopPortal(executionId, portalDesc.vmAddress());
                }
            }
        }

        var destroyChannelsOp = destroyChannels(executionId);
        if (destroyChannelsOp != null) {
            var opId = destroyChannelsOp.getId();
            var channelManagerOpsClient = newBlockingClient(
                LongRunningServiceGrpc.newBlockingStub(channelManagerChannel), APP,
                () -> internalUserCredentials.get().token());

            destroyChannelsOp = awaitOperationDone(channelManagerOpsClient, opId, Duration.ofSeconds(5));

            if (!destroyChannelsOp.getDone()) {
                LOG.warn("Cannot wait channel manager destroy all execution channels: { executionId: {} }",
                    executionId);
            }
        }

        if (portalDesc != null && portalDesc.vmId() != null) {
            freeVm(executionId, portalDesc.vmId());
        }

        if (portalDesc != null && portalDesc.allocatorSessionId() != null) {
            deleteSession(executionId, portalDesc.allocatorSessionId());
        }

        try {
            withRetries(LOG, () -> {
                try (var tx = TransactionHandle.create(storage)) {
                    executionDao.setCompletedExecutionStatus(executionId, tx);
                    operationDao.updateResponse(completeOperation.id(), Any.pack(
                        LzyPortalApi.FinishResponse.getDefaultInstance()).toByteArray(), tx);

                    LOG.info("Execution was completed: { executionId: {} }", executionId);
                    tx.commit();
                } catch (Exception e) {
                    LOG.warn("Cannot update execution status: { executionId: {} }", executionId, e);
                    operationDao.failOperation(completeOperation.id(), toProto(
                        Status.INTERNAL.withDescription("Cannot set response")), LOG);
                }
            });
        } catch (Exception e) {
            LOG.warn("Cannot update execution status: { executionId: {} }", executionId, e);
            operationDao.failOperation(completeOperation.id(), toProto(
                Status.INTERNAL.withDescription("Cannot set response")), LOG);
        }
    }

    public void cleanExecution(String executionId) {
        LOG.info("Attempt to clean execution: { executionId: {} }", executionId);

        try {
            withRetries(LOG, () -> executionDao.setCompletingExecutionStatus(executionId, null));
        } catch (Exception e) {
            LOG.warn("Cannot mark execution as completing: { executionId: {}, error: {} }",
                executionId, e.getMessage(), e);
            return;
        }

        stopGraphs(executionId);

        var portalDesc = getPortalDescription(executionId);

        if (portalDesc != null && portalDesc.vmAddress() != null) {
            stopPortal(executionId, portalDesc.vmAddress());
        }

        destroyChannels(executionId);

        if (portalDesc != null && portalDesc.vmId() != null) {
            freeVm(executionId, portalDesc.vmId());
        }

        if (portalDesc != null && portalDesc.allocatorSessionId() != null) {
            deleteSession(executionId, portalDesc.allocatorSessionId());
        }

        try {
            withRetries(LOG, () -> executionDao.setCompletedExecutionStatus(executionId, null));
            LOG.info("Execution was cleaned: { executionId: {} }", executionId);
        } catch (Exception e) {
            LOG.warn("Cannot update execution status: { executionId: {} }", executionId, e);
        }
    }

    @Nullable
    public LongRunning.Operation destroyChannels(String executionId) {
        LOG.info("Destroy channels of execution: { executionId: {} }", executionId);

        try {
            return channelManagerClient.destroyAll(LCMPS.ChannelDestroyAllRequest.newBuilder()
                .setExecutionId(executionId).build());
        } catch (StatusRuntimeException e) {
            LOG.warn("Cannot destroy channels of execution: { executionId: {} }", executionId, e);
        }

        return null;
    }

    private void stopGraphs(String executionId) {
        LOG.info("Attempt to stop graphs of execution because of completing: { executionId: {} }", executionId);

        String curGraphId = null;
        try {
            var graphs = withRetries(LOG, () -> graphDao.getAll(executionId));
            for (var graph : graphs) {
                curGraphId = graph.graphId();
                //noinspection ResultOfMethodCallIgnored
                graphExecutorClient.stop(GraphExecutorApi.GraphStopRequest.newBuilder().setWorkflowId(executionId)
                    .setGraphId(curGraphId).build());
            }
        } catch (StatusRuntimeException e) {
            LOG.warn("Cannot stop graphs of completed execution: { executionId: {}, graphId: {} }, error: {}",
                executionId, Objects.toString(curGraphId, ""), e.getStatus().getDescription());
        } catch (Exception e) {
            LOG.warn("Cannot find graphs of execution: { executionId: {} }", executionId);
        }
    }

    @Nullable
    private PortalDescription getPortalDescription(String executionId) {
        PortalDescription portalDesc = null;
        try {
            portalDesc = withRetries(LOG, () -> executionDao.getPortalDescription(executionId));
        } catch (Exception e) {
            LOG.warn("Cannot get portal for execution: { executionId: {} }", executionId, e);
        }
        return portalDesc;
    }

    @Nullable
    public LongRunning.Operation shutdownPortal(String executionId, HostAndPort portalVmAddress) {
        LOG.info("Attempt to shutdown portal of execution: { executionId: {} }", executionId);

        var portalClient = obtainPortalClient(executionId, portalVmAddress);

        try {
            LOG.info("Shutdown portal of execution: { executionId: {} }", executionId);
            var operation = portalClient.finish(LzyPortalApi.FinishRequest.getDefaultInstance());

            withRetries(LOG, () -> executionDao.updatePortalVmAddress(executionId, null, null, null));

            LOG.info("Portal of execution was shutdown: { executionId: {} }", executionId);
            return operation;
        } catch (Exception e) {
            LOG.warn("Cannot shutdown portal of execution: { executionId: {} }", executionId, e);
        }

        return null;
    }

    public void stopPortal(String executionId, HostAndPort portalVmAddress) {
        LOG.info("Attempt to stop portal of execution: { executionId: {} }", executionId);

        var portalClient = obtainPortalClient(executionId, portalVmAddress);

        try {
            LOG.info("Stop portal of execution: { executionId: {} }", executionId);
            //noinspection ResultOfMethodCallIgnored
            portalClient.stop(Empty.getDefaultInstance());

            withRetries(LOG, () -> executionDao.updatePortalVmAddress(executionId, null, null, null));

            LOG.info("Portal of execution was stopped: { executionId: {} }", executionId);
        } catch (Exception e) {
            LOG.warn("Cannot stop portal for execution: { executionId: {} }", executionId, e);
        }
    }

    public void freeVm(String executionId, String portalVmId) {
        LOG.info("Free portal vm of execution: { vmId: {}, executionId: {} }", portalVmId, executionId);

        try {
            //noinspection ResultOfMethodCallIgnored
            allocatorClient.free(VmAllocatorApi.FreeRequest.newBuilder()
                .setVmId(portalVmId)
                .build());

            withRetries(LOG, () -> executionDao.updateAllocateOperationData(executionId, null, null, null));

            LOG.info("Portal vm of execution was released: { vmId: {}, executionId: {} }", portalVmId, executionId);
        } catch (Exception e) {
            LOG.warn("Cannot free portal vm of execution: { vmId: {}, executionId: {} }", portalVmId, executionId, e);
        }
    }

    public void deleteSession(String executionId, String sessionId) {
        LOG.info("Delete allocator session for execution: { sessionId: {}, executionId: {} }", sessionId, executionId);

        try {
            //noinspection ResultOfMethodCallIgnored
            allocatorClient.deleteSession(VmAllocatorApi.DeleteSessionRequest.newBuilder()
                .setSessionId(sessionId)
                .build());

            withRetries(LOG, () -> executionDao.updatePortalVmAllocateSession(executionId, null, null, null));

            LOG.info("Allocator session for execution was deleted: { sessionId: {}, executionId: {} }",
                sessionId, executionId);
        } catch (Exception e) {
            LOG.warn("Cannot delete allocator session for execution: { sessionId: {}, executionId: {} }",
                sessionId, executionId, e);
        }
    }

    private LzyPortalGrpc.LzyPortalBlockingStub obtainPortalClient(String executionId, HostAndPort portalVmAddress) {
        var grpcChannel = portalChannels.computeIfAbsent(executionId, exId ->
            newGrpcChannel(portalVmAddress, LzyPortalGrpc.SERVICE_NAME));
        return newBlockingClient(LzyPortalGrpc.newBlockingStub(grpcChannel), APP,
            () -> internalUserCredentials.get().token());
    }
}
