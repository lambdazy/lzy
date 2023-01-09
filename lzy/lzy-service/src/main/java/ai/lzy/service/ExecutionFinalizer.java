package ai.lzy.service;

import ai.lzy.longrunning.Operation;
import ai.lzy.service.data.dao.ExecutionDao;
import ai.lzy.service.data.dao.GraphDao;
import ai.lzy.service.data.dao.PortalDescription;
import ai.lzy.service.data.dao.WorkflowDao;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.v1.AllocatorGrpc;
import ai.lzy.v1.VmAllocatorApi;
import ai.lzy.v1.channel.LzyChannelManagerPrivateGrpc;
import ai.lzy.v1.graph.GraphExecutorApi;
import ai.lzy.v1.graph.GraphExecutorGrpc;
import ai.lzy.v1.longrunning.LongRunning;
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

import java.util.Map;
import java.util.Objects;

import static ai.lzy.model.db.DbHelper.defaultRetryPolicy;
import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.service.LzyService.APP;
import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;

@Singleton
public class ExecutionFinalizer {
    private static final Logger LOG = LogManager.getLogger(ExecutionFinalizer.class);

    private final WorkflowDao workflowDao;
    private final ExecutionDao executionDao;
    private final GraphDao graphDao;

    private final RenewableJwt internalUserCredentials;

    private final Map<String, ManagedChannel> portalChannels;
    private final LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateBlockingStub channelManagerClient;
    private final GraphExecutorGrpc.GraphExecutorBlockingStub graphExecutorClient;
    private final AllocatorGrpc.AllocatorBlockingStub allocatorClient;

    public ExecutionFinalizer(WorkflowDao workflowDao, ExecutionDao executionDao, GraphDao graphDao,
                              @Named("LzyServiceIamToken") RenewableJwt internalUserCredentials,
                              @Named("PortalChannels") Map<String, ManagedChannel> portalChannels,
                              @Named("ChannelManagerServiceChannel") ManagedChannel channelManagerChannel,
                              @Named("GraphExecutorServiceChannel") ManagedChannel graphExecutorChannel,
                              @Named("AllocatorServiceChannel") ManagedChannel allocatorChannel)
    {
        this.workflowDao = workflowDao;
        this.executionDao = executionDao;
        this.graphDao = graphDao;

        this.internalUserCredentials = internalUserCredentials;

        this.portalChannels = portalChannels;
        this.channelManagerClient = newBlockingClient(
            LzyChannelManagerPrivateGrpc.newBlockingStub(channelManagerChannel), APP,
            () -> internalUserCredentials.get().token());
        this.graphExecutorClient = newBlockingClient(
            GraphExecutorGrpc.newBlockingStub(graphExecutorChannel), APP,
            () -> internalUserCredentials.get().token());
        this.allocatorClient = newBlockingClient(
            AllocatorGrpc.newBlockingStub(allocatorChannel), APP, () -> internalUserCredentials.get().token());
    }

    public Operation finalizeAndAwait(String executionId) {
        stopGraphs(executionId);

        var portalDesc = getPortalDescription(executionId);

        if (portalDesc != null && portalDesc.vmAddress() != null) {
            shutdownPortal(executionId, portalDesc.vmAddress(), false);
        }

        if (portalDesc != null && portalDesc.vmId() != null) {
            freeVm(executionId, portalDesc.vmId());
        }

        if (portalDesc != null && portalDesc.allocatorSessionId() != null) {
            deleteSession(executionId, portalDesc.allocatorSessionId());
        }

        try {
            withRetries(defaultRetryPolicy(), LOG, () -> executionDao.setDeadExecutionStatus(executionId, null));
            LOG.info("Execution {} is deleted", executionId);
        } catch (Exception e) {
            LOG.warn("Cannot update execution status {}", executionId, e);
        }

        destroyChannels(executionId);

        // after this action there is only data about snapshots and graphs of execution are presented in lzy-service db
        // todo: return operation that wraps shutdownPortal and destroyChannel
        return Operation.createCompleted("1", "", "", null, null, Any.getDefaultInstance());
    }

    public void finalizeNow(String executionId) {
        stopGraphs(executionId);

        var portalDesc = getPortalDescription(executionId);

        if (portalDesc != null && portalDesc.vmAddress() != null) {
            shutdownPortal(executionId, portalDesc.vmAddress(), true);
        }

        if (portalDesc != null && portalDesc.vmId() != null) {
            freeVm(executionId, portalDesc.vmId());
        }

        if (portalDesc != null && portalDesc.allocatorSessionId() != null) {
            deleteSession(executionId, portalDesc.allocatorSessionId());
        }

        try {
            withRetries(defaultRetryPolicy(), LOG, () -> executionDao.setDeadExecutionStatus(executionId, null));
            LOG.info("Execution {} is deleted", executionId);
        } catch (Exception e) {
            LOG.warn("Cannot update execution status {}", executionId, e);
        }

        destroyChannels(executionId);

        // after this action there is only data about snapshots and graphs of execution are presented in lzy-service db
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

    public void shutdownPortal(String executionId, HostAndPort portalVmAddress, boolean force) {
        var grpcChannel = portalChannels.computeIfAbsent(executionId, exId ->
            newGrpcChannel(portalVmAddress, LzyPortalGrpc.SERVICE_NAME));
        var portalClient = newBlockingClient(LzyPortalGrpc.newBlockingStub(grpcChannel), APP,
            () -> internalUserCredentials.get().token());

        try {
            if (force) {
                LOG.info("Stop portal {} for execution {}", portalVmAddress, executionId);
                portalClient.stop(Empty.getDefaultInstance());
            } else {
                LOG.info("Finish portal {} for execution {}", portalVmAddress, executionId);
                portalClient.finish(LzyPortalApi.FinishRequest.getDefaultInstance());
            }

            withRetries(LOG, () -> executionDao.updateAllocatedVmAddress(executionId, null, null, null));

            LOG.info("Portal {} stopped for execution {}", portalVmAddress, executionId);
        } catch (Exception e) {
            LOG.warn("Cannot clean portal for execution {}", executionId, e);
        }
    }

    public void freeVm(String executionId, String portalVmId) {
        try {
            LOG.info("Freeing portal vm {} for execution {}", portalVmId, executionId);

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
            allocatorClient.deleteSession(VmAllocatorApi.DeleteSessionRequest.newBuilder()
                .setSessionId(sessionId)
                .build());

            withRetries(LOG, () -> executionDao.updateAllocatorSession(executionId, null, null, null));

            LOG.info("Allocator session {} cleaned for execution {}", sessionId, executionId);
        } catch (Exception e) {
            LOG.warn("Cannot clean allocator session for execution {}", executionId, e);
        }
    }
}
