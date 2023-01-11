package ai.lzy.service;

import ai.lzy.longrunning.Operation;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.service.data.dao.ExecutionDao;
import ai.lzy.service.data.dao.GraphDao;
import ai.lzy.service.data.dao.PortalDescription;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.v1.AllocatorGrpc;
import ai.lzy.v1.VmAllocatorApi;
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
public class ExecutionFinalizer {
    private static final Logger LOG = LogManager.getLogger(ExecutionFinalizer.class);

    private final ExecutionDao executionDao;
    private final GraphDao graphDao;
    private final OperationDao operationDao;

    private final RenewableJwt internalUserCredentials;

    private final ManagedChannel channelManagerChannel;
    private final Map<String, ManagedChannel> portalChannels;
    private final LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateBlockingStub channelManagerClient;
    private final GraphExecutorGrpc.GraphExecutorBlockingStub graphExecutorClient;
    private final AllocatorGrpc.AllocatorBlockingStub allocatorClient;

    public ExecutionFinalizer(ExecutionDao executionDao, GraphDao graphDao,
                              @Named("LzyServiceOperationDao") OperationDao operationDao,
                              @Named("LzyServiceIamToken") RenewableJwt internalUserCredentials,
                              @Named("ChannelManagerServiceChannel") ManagedChannel channelManagerChannel,
                              @Named("PortalChannels") Map<String, ManagedChannel> portalChannels,
                              @Named("GraphExecutorServiceChannel") ManagedChannel graphExecutorChannel,
                              @Named("AllocatorServiceChannel") ManagedChannel allocatorChannel)
    {
        this.executionDao = executionDao;
        this.graphDao = graphDao;
        this.operationDao = operationDao;

        this.internalUserCredentials = internalUserCredentials;

        this.channelManagerChannel = channelManagerChannel;
        this.portalChannels = portalChannels;
        this.channelManagerClient = newBlockingClient(
            LzyChannelManagerPrivateGrpc.newBlockingStub(channelManagerChannel), APP,
            () -> internalUserCredentials.get().token());
        this.graphExecutorClient = newBlockingClient(
            GraphExecutorGrpc.newBlockingStub(graphExecutorChannel), APP, () -> internalUserCredentials.get().token());
        this.allocatorClient = newBlockingClient(
            AllocatorGrpc.newBlockingStub(allocatorChannel), APP, () -> internalUserCredentials.get().token());
    }

    public void gracefulFinalize(String executionId, Operation finalizeOp) {
        // after this action there is only data about snapshots and graphs of execution are presented in lzy-service db

        stopGraphs(executionId);

        var portalDesc = getPortalDescription(executionId);

        if (portalDesc != null && portalDesc.vmAddress() != null) {
            var finishOp = finishPortal(executionId, portalDesc.vmAddress());
            if (finishOp != null) {
                var grpcChannel = portalChannels.computeIfAbsent(executionId, exId ->
                    newGrpcChannel(portalDesc.vmAddress(), LzyPortalGrpc.SERVICE_NAME));
                var portalOpsClient = newBlockingClient(
                    LongRunningServiceGrpc.newBlockingStub(grpcChannel), APP,
                    () -> internalUserCredentials.get().token());

                finishOp = awaitOperationDone(portalOpsClient, finishOp.getId(), Duration.ofSeconds(5));

                if (!finishOp.getDone()) {
                    LOG.warn("Cannot wait portal of execution finished: { executionId: {} }", executionId);
                }
            }
        }

        if (portalDesc != null && portalDesc.vmId() != null) {
            freeVm(executionId, portalDesc.vmId());
        }

        if (portalDesc != null && portalDesc.allocatorSessionId() != null) {
            deleteSession(executionId, portalDesc.allocatorSessionId());
        }

        try {
            withRetries(LOG, () -> executionDao.setDeadExecutionStatus(executionId, null));
            LOG.info("Execution {} is deleted", executionId);
        } catch (Exception e) {
            LOG.warn("Cannot update execution status {}", executionId, e);
        }

        var destroyChannelsOp = destroyChannels(executionId);
        if (destroyChannelsOp != null) {
            var opId = destroyChannelsOp.getId();
            var channelManagerOpsClient = newBlockingClient(
                LongRunningServiceGrpc.newBlockingStub(channelManagerChannel), APP,
                () -> internalUserCredentials.get().token());

            destroyChannelsOp = awaitOperationDone(channelManagerOpsClient, opId, Duration.ofSeconds(5));

            if (!destroyChannelsOp.getDone()) {
                LOG.warn("Cannot wait channel manager destroy all execution resources: { executionId: {} }",
                    executionId);
            }
        }

        try {
            withRetries(LOG, () -> operationDao.updateResponse(finalizeOp.id(), Any.pack(
                LzyPortalApi.FinishResponse.getDefaultInstance()).toByteArray(), null));
        } catch (Exception e) {
            LOG.error("Cannot set finish execution operation response: { executionId: {} }", executionId, e);
            operationDao.failOperation(finalizeOp.id(), toProto(Status.INTERNAL.withDescription("Cannot set response")),
                LOG);
        }
    }

    public void finalizeNow(String executionId) {
        // after this action there is only data about snapshots and graphs of execution are presented in lzy-service db

        stopGraphs(executionId);

        var portalDesc = getPortalDescription(executionId);

        if (portalDesc != null && portalDesc.vmAddress() != null) {
            stopPortal(executionId, portalDesc.vmAddress());
        }

        if (portalDesc != null && portalDesc.vmId() != null) {
            freeVm(executionId, portalDesc.vmId());
        }

        if (portalDesc != null && portalDesc.allocatorSessionId() != null) {
            deleteSession(executionId, portalDesc.allocatorSessionId());
        }

        try {
            withRetries(LOG, () -> executionDao.setDeadExecutionStatus(executionId, null));
            LOG.info("Execution {} is deleted", executionId);
        } catch (Exception e) {
            LOG.warn("Cannot update execution status {}", executionId, e);
        }

        //noinspection ResultOfMethodCallIgnored
        destroyChannels(executionId);
    }

    public void finishInDao(String userId, String executionId, Status status) throws Exception {
        withRetries(LOG, () -> executionDao.updateFinishData(userId, executionId, status, null));
    }

    private LongRunning.Operation destroyChannels(String executionId) {
        /*
        try {
            return channelManagerClient.destroyAll(LCMPS.ChannelDestroyAllRequest.newBuilder()
                .setExecutionId(executionId).build());
        } catch (StatusRuntimeException e) {
            LOG.warn("Cannot destroy channels: { executionId: {}, error: {} }", executionId,
                e.getStatus().getDescription());
        }
        */

        return null;
    }

    private void stopGraphs(String executionId) {
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
            LOG.warn("Cannot stop graph: { executionId: {}, graphId: {} }, error: {}", executionId,
                Objects.toString(curGraphId, ""), e.getStatus().getDescription());
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
            LOG.warn("Cannot get portal for execution {}", executionId, e);
        }
        return portalDesc;
    }

    @Nullable
    public LongRunning.Operation finishPortal(String executionId, HostAndPort portalVmAddress) {
        var portalClient = obtainPortalClient(executionId, portalVmAddress);

        try {
            LOG.info("Finish portal {} for execution {}", portalVmAddress, executionId);
            var finishPortalOp = portalClient.finish(LzyPortalApi.FinishRequest.getDefaultInstance());

            withRetries(LOG, () -> executionDao.updatePortalVmAddress(executionId, null, null, null));

            LOG.info("Portal {} stopped for execution {}", portalVmAddress, executionId);
            return finishPortalOp;
        } catch (Exception e) {
            LOG.warn("Cannot clean portal for execution {}", executionId, e);
        }

        return null;
    }

    public void stopPortal(String executionId, HostAndPort portalVmAddress) {
        var portalClient = obtainPortalClient(executionId, portalVmAddress);

        try {
            LOG.info("Stop portal for execution: { vmAddress: {}, executionId: {} }", portalVmAddress, executionId);
            //noinspection ResultOfMethodCallIgnored
            portalClient.stop(Empty.getDefaultInstance());

            withRetries(LOG, () -> executionDao.updatePortalVmAddress(executionId, null, null, null));

            LOG.info("Portal stopped for execution: { vmAddress: {}, executionId: {} }", portalVmAddress, executionId);
        } catch (Exception e) {
            LOG.warn("Cannot stop portal for execution: { executionId: {} }", executionId, e);
        }
    }

    public void freeVm(String executionId, String portalVmId) {
        try {
            LOG.info("Freeing portal vm {} for execution {}", portalVmId, executionId);
            //noinspection ResultOfMethodCallIgnored
            allocatorClient.free(VmAllocatorApi.FreeRequest.newBuilder()
                .setVmId(portalVmId)
                .build());

            withRetries(LOG, () -> executionDao.updateAllocateOperationData(executionId, null, null, null));

            LOG.info("Portal vm cleaned {} for execution {}", portalVmId, executionId);
        } catch (Exception e) {
            LOG.warn("Cannot free portal vm for execution {}", executionId, e);
        }
    }

    public void deleteSession(String executionId, String sessionId) {
        try {
            LOG.info("Cleaning allocator session {} for execution {}", sessionId, executionId);
            //noinspection ResultOfMethodCallIgnored
            allocatorClient.deleteSession(VmAllocatorApi.DeleteSessionRequest.newBuilder()
                .setSessionId(sessionId)
                .build());

            withRetries(LOG, () -> executionDao.updatePortalVmAllocateSession(executionId, null, null, null));

            LOG.info("Allocator session {} cleaned for execution {}", sessionId, executionId);
        } catch (Exception e) {
            LOG.warn("Cannot clean allocator session for execution {}", executionId, e);
        }
    }

    private LzyPortalGrpc.LzyPortalBlockingStub obtainPortalClient(String executionId, HostAndPort portalVmAddress) {
        var grpcChannel = portalChannels.computeIfAbsent(executionId, exId ->
            newGrpcChannel(portalVmAddress, LzyPortalGrpc.SERVICE_NAME));
        return newBlockingClient(LzyPortalGrpc.newBlockingStub(grpcChannel), APP,
            () -> internalUserCredentials.get().token());
    }
}
