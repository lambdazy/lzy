package ai.lzy.service;

import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.iam.resources.subjects.AuthProvider;
import ai.lzy.iam.resources.subjects.Worker;
import ai.lzy.longrunning.Operation;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.service.data.KafkaTopicDesc;
import ai.lzy.service.data.dao.ExecutionDao;
import ai.lzy.service.data.dao.GraphDao;
import ai.lzy.service.data.dao.PortalDescription;
import ai.lzy.service.data.dao.WorkflowDao;
import ai.lzy.service.data.storage.LzyServiceStorage;
import ai.lzy.service.kafka.KafkaLogsListeners;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.util.kafka.KafkaAdminClient;
import ai.lzy.v1.AllocatorGrpc;
import ai.lzy.v1.VmAllocatorApi;
import ai.lzy.v1.channel.LCMPS;
import ai.lzy.v1.channel.LzyChannelManagerPrivateGrpc;
import ai.lzy.v1.graph.GraphExecutorApi;
import ai.lzy.v1.graph.GraphExecutorGrpc;
import ai.lzy.v1.kafka.KafkaS3Sink;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import ai.lzy.v1.portal.LzyPortalApi;
import ai.lzy.v1.workflow.LWFS;
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

import java.sql.SQLException;
import java.time.Duration;
import java.util.Objects;

import static ai.lzy.longrunning.OperationUtils.awaitOperationDone;
import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.service.LzyService.APP;
import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.withIdempotencyKey;
import static ai.lzy.util.grpc.ProtoConverter.toProto;

@Singleton
public class CleanExecutionCompanion {
    private static final Logger LOG = LogManager.getLogger(CleanExecutionCompanion.class);

    private final LzyServiceStorage storage;
    private final WorkflowDao workflowDao;
    private final ExecutionDao executionDao;
    private final GraphDao graphDao;
    private final OperationDao operationDao;

    private final RenewableJwt internalUserCredentials;

    private final PortalClientProvider portalClients;
    private final ManagedChannel channelManagerChannel;
    private final SubjectServiceGrpcClient subjectClient;
    private final LzyServiceMetrics metrics;
    private final LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateBlockingStub channelManagerClient;
    private final GraphExecutorGrpc.GraphExecutorBlockingStub graphExecutorClient;
    private final AllocatorGrpc.AllocatorBlockingStub allocatorClient;

    @Nullable
    private final KafkaAdminClient kafkaAdminClient;
    private final KafkaLogsListeners kafkaLogsListeners;
    private final BeanFactory.S3SinkClient s3SinkClient;

    public CleanExecutionCompanion(PortalClientProvider portalClients, LzyServiceStorage storage,
                                   WorkflowDao workflowDao, ExecutionDao executionDao, GraphDao graphDao,
                                   @Named("LzyServiceOperationDao") OperationDao operationDao,
                                   @Named("LzyServiceIamToken") RenewableJwt internalUserCredentials,
                                   @Named("ChannelManagerServiceChannel") ManagedChannel channelManagerChannel,
                                   @Named("GraphExecutorServiceChannel") ManagedChannel graphExecutorChannel,
                                   @Named("AllocatorServiceChannel") ManagedChannel allocatorChannel,
                                   @Named("LzySubjectServiceClient") SubjectServiceGrpcClient subjectClient,
                                   LzyServiceMetrics metrics,
                                   @Named("LzyServiceKafkaAdminClient") KafkaAdminClient kafkaAdminClient,
                                   KafkaLogsListeners kafkaLogsListeners,
                                   BeanFactory.S3SinkClient s3SinkClient)
    {
        this.storage = storage;
        this.workflowDao = workflowDao;
        this.executionDao = executionDao;
        this.graphDao = graphDao;
        this.operationDao = operationDao;

        this.internalUserCredentials = internalUserCredentials;

        this.portalClients = portalClients;
        this.channelManagerChannel = channelManagerChannel;
        this.subjectClient = subjectClient;
        this.metrics = metrics;
        this.kafkaAdminClient = kafkaAdminClient;
        this.kafkaLogsListeners = kafkaLogsListeners;
        this.channelManagerClient = newBlockingClient(
            LzyChannelManagerPrivateGrpc.newBlockingStub(channelManagerChannel), APP,
            () -> internalUserCredentials.get().token());
        this.graphExecutorClient = newBlockingClient(
            GraphExecutorGrpc.newBlockingStub(graphExecutorChannel), APP, () -> internalUserCredentials.get().token());
        this.allocatorClient = newBlockingClient(
            AllocatorGrpc.newBlockingStub(allocatorChannel), APP, () -> internalUserCredentials.get().token());

        this.s3SinkClient = s3SinkClient;
    }

    public void finishWorkflow(String userId, @Nullable String workflowName, String executionId, Status reason)
        throws Exception
    {
        LOG.info("Attempt to mark execution as broken by reason: { executionId: {}, reason: {} }", executionId, reason);

        withRetries(LOG, () -> {
            try (var tx = TransactionHandle.create(storage)) {
                if (workflowName == null) {
                    workflowDao.setActiveExecutionToNull(userId, executionId, tx);
                } else {
                    workflowDao.setActiveExecutionToNull(userId, workflowName, executionId, tx);
                }
                executionDao.updateFinishData(userId, executionId, reason, tx);
                executionDao.setErrorExecutionStatus(executionId, tx);

                tx.commit();
            }
        });

        metrics.activeExecutions.labels(userId).dec();
    }

    public boolean tryToFinishExecution(String userId, String executionId, Status reason) {
        LOG.info("Attempt to mark execution as broken by reason: { executionId: {}, reason: {} }", executionId, reason);

        try {
            withRetries(LOG, () -> {
                try (var tx = TransactionHandle.create(storage)) {
                    executionDao.updateFinishData(userId, executionId, reason, tx);
                    executionDao.setErrorExecutionStatus(executionId, tx);

                    tx.commit();
                }
            });
        } catch (Exception e) {
            LOG.warn("Cannot mark execution as broken: { executionId: {}, error: {} }", executionId, e.getMessage());
            return false;
        }

        metrics.activeExecutions.labels(userId).dec();
        return true;
    }

    public boolean tryToFinishWorkflow(String userId, String workflowName, String executionId, Status reason) {
        LOG.info("Attempt to mark workflow execution as broken by reason: " +
            "{ workflowName: {}, executionId: {}, reason: {} }", workflowName, executionId, reason);

        try {
            withRetries(LOG, () -> {
                try (var tx = TransactionHandle.create(storage)) {
                    if (workflowName == null) {
                        workflowDao.setActiveExecutionToNull(userId, executionId, tx);
                    } else {
                        workflowDao.setActiveExecutionToNull(userId, workflowName, executionId, tx);
                    }
                    executionDao.updateFinishData(userId, executionId, reason, tx);
                    executionDao.setErrorExecutionStatus(executionId, tx);

                    tx.commit();
                }
            });
        } catch (Exception e) {
            LOG.warn("Cannot mark workflow execution as broken: { workflowName: {}, executionId: {}, error: {} }",
                workflowName, executionId, e.getMessage());
            return false;
        }

        metrics.activeExecutions.labels(userId).dec();
        return true;
    }

    public void completeExecution(String userId, String executionId, Operation completeOperation) {
        LOG.info("Attempt to complete execution: { executionId: {} }", executionId);

        stopGraphs(executionId);

        var portalDesc = getPortalDescription(executionId);

        if (portalDesc != null && portalDesc.vmAddress() != null) {
            var shutdownPortalOp = shutdownPortal(executionId, portalDesc.vmAddress());
            if (shutdownPortalOp != null) {
                var portalOpsClient = portalClients.getOperationsGrpcClient(executionId, portalDesc.vmAddress());

                try {
                    // it may be long-running process to finish stdout/err portal slots
                    shutdownPortalOp = awaitOperationDone(portalOpsClient, shutdownPortalOp.getId(),
                        Duration.ofMinutes(5));

                    if (!shutdownPortalOp.getDone()) {
                        LOG.warn("Cannot wait portal of execution shutdown: { executionId: {}, error: timeout }",
                            executionId);
                    }
                } catch (Exception e) {
                    LOG.warn("Cannot wait portal of execution shutdown: { executionId: {}, error: {} }",
                        executionId, e.getMessage(), e);
                }

                stopPortal(executionId, portalDesc.vmAddress());
            }
        }

        var destroyChannelsOp = destroyChannels(executionId);
        if (destroyChannelsOp != null) {
            var opId = destroyChannelsOp.getId();
            var channelManagerOpsClient = newBlockingClient(
                LongRunningServiceGrpc.newBlockingStub(channelManagerChannel), APP,
                () -> internalUserCredentials.get().token());

            try {
                destroyChannelsOp = awaitOperationDone(channelManagerOpsClient, opId, Duration.ofSeconds(10));

                if (!destroyChannelsOp.getDone()) {
                    LOG.warn("Cannot wait channel manager destroy all execution channels: " +
                        "{ executionId: {}, error: timeout }", executionId);
                }
            } catch (Exception e) {
                LOG.warn("Cannot wait channel manager destroy all execution channels: { executionId: {}, error: {} }",
                    executionId, e.getMessage(), e);
            }
        }

        if (portalDesc != null && portalDesc.vmId() != null) {
            freeVm(executionId, portalDesc.vmId());
        }

        if (portalDesc != null && portalDesc.allocatorSessionId() != null) {
            deleteSession(executionId, portalDesc.allocatorSessionId());
        }

        if (portalDesc != null && portalDesc.subjectId() != null) {
            removePortalSubject(executionId, portalDesc.subjectId(), portalDesc.portalId());
        }

        kafkaLogsListeners.notifyFinished(executionId);
        dropKafkaResources(executionId);

        try {
            withRetries(LOG, () -> {
                try (var tx = TransactionHandle.create(storage)) {
                    executionDao.setCompletedExecutionStatus(executionId, tx);
                    operationDao.complete(completeOperation.id(), Any.pack(
                        LWFS.FinishWorkflowResponse.getDefaultInstance()), tx);
                    tx.commit();
                }
            });

            LOG.info("Execution was completed: { executionId: {} }", executionId);

            metrics.activeExecutions.labels(userId).dec();
        } catch (Exception e) {
            LOG.warn("Cannot update execution status: { executionId: {} }", executionId, e);
            try {
                operationDao.failOperation(completeOperation.id(), toProto(
                    Status.INTERNAL.withDescription("Cannot set response")), null, LOG);
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
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

        var destroyChannelsOp = destroyChannels(executionId);
        if (destroyChannelsOp != null) {
            var opId = destroyChannelsOp.getId();
            var channelManagerOpsClient = newBlockingClient(
                LongRunningServiceGrpc.newBlockingStub(channelManagerChannel), APP,
                () -> internalUserCredentials.get().token());

            try {
                destroyChannelsOp = awaitOperationDone(channelManagerOpsClient, opId, Duration.ofSeconds(10));

                if (!destroyChannelsOp.getDone()) {
                    LOG.warn("Cannot wait channel manager destroy all execution channels: " +
                        "{ executionId: {}, error: timeout }", executionId);
                }
            } catch (Exception e) {
                LOG.warn("Cannot wait channel manager destroy all execution channels: { executionId: {}, error: {} }",
                    executionId, e.getMessage(), e);
            }
        }

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

        if (portalDesc != null && portalDesc.subjectId() != null) {
            removePortalSubject(executionId, portalDesc.subjectId(), portalDesc.portalId());
        }

        if (portalDesc != null && portalDesc.subjectId() != null) {
            try {
                subjectClient.removeSubject(
                    new Worker(portalDesc.subjectId(), AuthProvider.INTERNAL, portalDesc.portalId()));
            } catch (Exception e) {
                LOG.warn("Cannot remove portal subject from iam: { executionId: {}, subjectId: {} }", executionId,
                    portalDesc.subjectId());
            }
        }


        kafkaLogsListeners.notifyFinished(executionId);
        dropKafkaResources(executionId);

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
            var idempotentChannelManagerClient = withIdempotencyKey(channelManagerClient, "destroyAll/" + executionId);

            return idempotentChannelManagerClient.destroyAll(LCMPS.ChannelDestroyAllRequest.newBuilder()
                .setExecutionId(executionId).build());
        } catch (StatusRuntimeException e) {
            LOG.warn("Cannot destroy channels of execution: { executionId: {}, error: {} }",
                executionId, e.getStatus());
        }

        return null;
    }

    void stopGraphs(String executionId) {
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
            LOG.warn("Cannot find graphs of execution: { executionId: {}, error: {} }", executionId, e.getMessage());
        }
    }

    @Nullable
    private PortalDescription getPortalDescription(String executionId) {
        PortalDescription portalDesc = null;
        try {
            portalDesc = withRetries(LOG, () -> executionDao.getPortalDescription(executionId));
        } catch (Exception e) {
            LOG.warn("Cannot get portal for execution: { executionId: {}, error: {} }", executionId, e.getMessage(), e);
        }
        return portalDesc;
    }

    @Nullable
    public LongRunning.Operation shutdownPortal(String executionId, HostAndPort portalVmAddress) {
        LOG.info("Attempt to shutdown portal of execution: { executionId: {} }", executionId);

        var portalClient = portalClients.getGrpcClient(executionId, portalVmAddress);

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

        var portalClient = portalClients.getGrpcClient(executionId, portalVmAddress);

        try {
            LOG.info("Stop portal of execution: { executionId: {} }", executionId);
            //noinspection ResultOfMethodCallIgnored
            portalClient.stop(Empty.getDefaultInstance());

            withRetries(LOG, () -> executionDao.updatePortalVmAddress(executionId, null, null, null));

            LOG.info("Portal of execution was stopped: { executionId: {} }", executionId);
        } catch (Exception e) {
            LOG.warn("Cannot stop portal for execution: { executionId: {}, error: {} }",
                executionId, e.getMessage(), e);
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

    private void dropKafkaResources(String executionId) {
        final KafkaTopicDesc kafkaDesc;
        try {
            kafkaDesc = withRetries(LOG, () -> executionDao.getKafkaTopicDesc(executionId, null));
        } catch (Exception e) {
            LOG.error("Cannot get kafka topic description from db for execution {}, please clear it", executionId, e);
            return;
        }

        LOG.debug("Deleting kafka topic {} and user {} for execution {}",
            kafkaDesc.topicName(), kafkaDesc.username(), executionId);

        try {
            kafkaAdminClient.dropUser(kafkaDesc.username());
        } catch (Exception ex) {
            LOG.error("Cannot remove kafka user for execution {}: ", executionId, ex);
        }

        if (s3SinkClient.enabled()) {  // Completing job, it will delete topic
            s3SinkClient.stub().stop(KafkaS3Sink.StopRequest.newBuilder()
                .setJobId(kafkaDesc.sinkTaskId())
                .build());
        }
    }

    public void removePortalSubject(String executionId, String subjectId, String subjectName) {
        LOG.debug("Remove portal iam subject: { executionId: {}, subjectId: {} }", executionId, subjectId);

        try {
            subjectClient.removeSubject(new Worker(subjectId, AuthProvider.INTERNAL, subjectName));
            withRetries(LOG, () -> executionDao.updatePortalSubjectId(executionId, null, null));
        } catch (Exception e) {
            LOG.warn("Cannot remove portal iam subject: { executionId: {}, subjectId: {} }", executionId, subjectId);
        }
    }
}
