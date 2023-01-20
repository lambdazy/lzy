package ai.lzy.service.graph;

import ai.lzy.longrunning.Operation;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.service.CleanExecutionCompanion;
import ai.lzy.service.PortalClientProvider;
import ai.lzy.service.data.dao.ExecutionDao;
import ai.lzy.service.data.dao.GraphDao;
import ai.lzy.service.data.dao.WorkflowDao;
import ai.lzy.service.graph.debug.InjectedFailures;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.v1.VmPoolServiceGrpc;
import ai.lzy.v1.channel.LzyChannelManagerPrivateGrpc;
import ai.lzy.v1.graph.GraphExecutorApi;
import ai.lzy.v1.graph.GraphExecutorGrpc;
import ai.lzy.v1.portal.LzyPortalApi;
import ai.lzy.v1.portal.LzyPortalApi.PortalSlotStatus.SnapshotSlotStatus;
import ai.lzy.v1.workflow.LWFS;
import ai.lzy.v1.workflow.LWFS.ExecuteGraphResponse;
import ai.lzy.v1.workflow.LWFS.StopGraphRequest;
import ai.lzy.v1.workflow.LWFS.StopGraphResponse;
import com.google.protobuf.Any;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import jakarta.annotation.Nullable;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.UUID;

import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.service.LzyService.APP;
import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.withIdempotencyKey;
import static ai.lzy.util.grpc.ProtoConverter.toProto;
import static ai.lzy.v1.graph.GraphExecutorGrpc.newBlockingStub;

@Singleton
public class GraphExecutionService {
    private static final Logger LOG = LogManager.getLogger(GraphExecutionService.class);

    private final WorkflowDao workflowDao;
    private final GraphDao graphDao;
    private final OperationDao operationDao;

    private final GraphValidator validator;
    private final GraphBuilder builder;

    private final CleanExecutionCompanion cleanExecutionCompanion;

    private final PortalClientProvider portalClients;
    private final GraphExecutorGrpc.GraphExecutorBlockingStub graphExecutorClient;

    public GraphExecutionService(GraphDao graphDao, WorkflowDao workflowDao, ExecutionDao executionDao,
                                 CleanExecutionCompanion cleanExecutionCompanion, PortalClientProvider portalClients,
                                 @Named("LzyServiceOperationDao") OperationDao operationDao,
                                 @Named("LzyServiceIamToken") RenewableJwt internalUserCredentials,
                                 @Named("AllocatorServiceChannel") ManagedChannel allocatorChannel,
                                 @Named("ChannelManagerServiceChannel") ManagedChannel channelManagerChannel,
                                 @Named("GraphExecutorServiceChannel") ManagedChannel graphExecutorChannel)
    {
        this.cleanExecutionCompanion = cleanExecutionCompanion;
        this.portalClients = portalClients;

        this.workflowDao = workflowDao;
        this.graphDao = graphDao;
        this.operationDao = operationDao;

        this.graphExecutorClient = newBlockingClient(
            newBlockingStub(graphExecutorChannel), APP, () -> internalUserCredentials.get().token());

        var vmPoolClient = newBlockingClient(
            VmPoolServiceGrpc.newBlockingStub(allocatorChannel), APP, () -> internalUserCredentials.get().token());

        this.validator = new GraphValidator(executionDao, vmPoolClient);

        var channelManagerClient = newBlockingClient(
            LzyChannelManagerPrivateGrpc.newBlockingStub(channelManagerChannel), APP,
            () -> internalUserCredentials.get().token());

        this.builder = new GraphBuilder(executionDao, channelManagerClient);
    }

    @Nullable
    public Operation executeGraph(GraphExecutionState state) {
        LOG.info("Start processing execute graph operation: { operationId: {} }", state.getOpId());

        String userId = state.getUserId();
        String executionId = state.getExecutionId();

        try {
            InjectedFailures.failExecuteGraph1();

            setWorkflowName(state);

            String workflowName = state.getWorkflowName();

            if (state.isInvalid()) {
                if (cleanExecutionCompanion.markExecutionAsBroken(userId, workflowName, executionId,
                    state.getErrorStatus()))
                {
                    cleanExecutionCompanion.cleanExecution(executionId);
                }
                return operationDao.failOperation(state.getOpId(), toProto(state.getErrorStatus()), null, LOG);
            }


            if (state.getDataFlowGraph() == null || state.getZone() == null) {
                LOG.debug("Validate dataflow graph, current state: " + state);
                validator.validate(state);

                InjectedFailures.failExecuteGraph2();

                withRetries(LOG, () -> graphDao.update(state, null));
            }

            if (state.isInvalid()) {
                if (cleanExecutionCompanion.markExecutionAsBroken(userId, workflowName, executionId,
                    state.getErrorStatus()))
                {
                    cleanExecutionCompanion.cleanExecution(executionId);
                }
                return operationDao.failOperation(state.getOpId(), toProto(state.getErrorStatus()), null, LOG);
            }

            LOG.info("Dataflow graph built and validated, building execution graph, current state:" + state);

            var portalClient = portalClients.getGrpcClient(executionId);

            if (portalClient == null) {
                LOG.error("Cannot get portal client while creating portal slots for current graph: " + state);
                var errorStatus = Status.INTERNAL.withDescription("Cannot build execution graph");
                if (cleanExecutionCompanion.markExecutionAsBroken(userId, workflowName, executionId, errorStatus)) {
                    cleanExecutionCompanion.cleanExecution(executionId);
                }
                return operationDao.failOperation(state.getOpId(), toProto(errorStatus), null, LOG);
            }

            InjectedFailures.failExecuteGraph3();

            if (state.getTasks() == null) {
                LOG.debug("Building graph, current state: " + state);
                builder.build(state, portalClient);

                InjectedFailures.failExecuteGraph4();

                withRetries(LOG, () -> graphDao.update(state, null));
            }

            if (state.isInvalid()) {
                if (cleanExecutionCompanion.markExecutionAsBroken(userId, workflowName, executionId,
                    state.getErrorStatus()))
                {
                    cleanExecutionCompanion.cleanExecution(executionId);
                }
                return operationDao.failOperation(state.getOpId(), toProto(state.getErrorStatus()), null, LOG);
            }

            LOG.info("Graph successfully built: " + state);

            if (state.getGraphId() == null) {
                LOG.debug("Send execute graph request to graph execution service, current state: " + state);

                InjectedFailures.failExecuteGraph5();

                if (state.getIdempotencyKey() == null) {
                    state.setIdempotencyKey(UUID.randomUUID().toString());
                    withRetries(LOG, () -> graphDao.update(state, null));
                }

                var idempotentGraphExecClient = withIdempotencyKey(graphExecutorClient, state.getIdempotencyKey());

                GraphExecutorApi.GraphExecuteResponse executeResponse;
                try {
                    executeResponse = idempotentGraphExecClient.execute(
                        GraphExecutorApi.GraphExecuteRequest.newBuilder()
                            .setWorkflowId(executionId)
                            .setWorkflowName(state.getWorkflowName())
                            .setUserId(userId)
                            .setParentGraphId(state.getParentGraphId())
                            .addAllTasks(state.getTasks())
                            .addAllChannels(state.getChannels())
                            .build());
                } catch (StatusRuntimeException e) {
                    if (cleanExecutionCompanion.markExecutionAsBroken(userId, workflowName, executionId,
                        e.getStatus()))
                    {
                        cleanExecutionCompanion.cleanExecution(executionId);
                    }
                    return operationDao.failOperation(state.getOpId(), toProto(e.getStatus()), null, LOG);
                }

                state.setGraphId(executeResponse.getStatus().getGraphId());

                InjectedFailures.failExecuteGraph6();

                withRetries(LOG, () -> graphDao.update(state, null));
            }

            LOG.info("Graph successfully executed, current state: " + state);

            InjectedFailures.failExecuteGraph7();

            try {
                withRetries(LOG, () -> graphDao.save(new GraphDao.GraphDescription(
                    state.getGraphId(), executionId, state.getPortalInputSlots()), null));
            } catch (Exception e) {
                LOG.error("Cannot save portal slots", e);

                var stopResponse = graphExecutorClient.stop(
                    GraphExecutorApi.GraphStopRequest.newBuilder().setGraphId(state.getGraphId()).build());

                var errorStatus = Status.INTERNAL.withDescription("Error while graph execution");
                if (cleanExecutionCompanion.markExecutionAsBroken(userId, workflowName, executionId, errorStatus)) {
                    cleanExecutionCompanion.cleanExecution(executionId);
                }
                return operationDao.failOperation(state.getOpId(), toProto(errorStatus), null, LOG);
            }

            InjectedFailures.failExecuteGraph8();

            var response = ExecuteGraphResponse.newBuilder()
                .setGraphId(state.getGraphId())
                .build();
            var packed = Any.pack(response);

            try {
                return withRetries(LOG, () -> operationDao.complete(state.getOpId(), packed, null));
            } catch (Exception e) {
                LOG.error("Error while executing transaction: {}", e.getMessage(), e);

                var stopResponse = graphExecutorClient.stop(
                    GraphExecutorApi.GraphStopRequest.newBuilder().setGraphId(state.getGraphId()).build());

                var errorStatus = Status.INTERNAL.withDescription("Error while execute graph: " + e.getMessage());
                if (cleanExecutionCompanion.markExecutionAsBroken(userId, workflowName, executionId, errorStatus)) {
                    cleanExecutionCompanion.cleanExecution(executionId);
                }
                return operationDao.failOperation(state.getOpId(), toProto(errorStatus), null, LOG);
            }
        } catch (InjectedFailures.TerminateException e) {
            LOG.error("Got InjectedFailure exception: " + e.getMessage());
            // don't fail operation explicitly, just pass
            return null;
        } catch (Exception e) {
            var errorStatus = Status.INTERNAL.withDescription("Error while execute graph: " + e.getMessage());
            if (cleanExecutionCompanion.markExecutionAsBroken(userId, state.getWorkflowName(), executionId,
                errorStatus))
            {
                cleanExecutionCompanion.cleanExecution(executionId);
            }
            try {
                return operationDao.failOperation(state.getOpId(), toProto(errorStatus), null, LOG);
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    public void graphStatus(LWFS.GraphStatusRequest request, StreamObserver<LWFS.GraphStatusResponse> response) {
        var executionId = request.getExecutionId();
        var graphId = request.getGraphId();

        GraphExecutorApi.GraphStatusResponse graphStatus;
        try {
            graphStatus = graphExecutorClient.status(GraphExecutorApi.GraphStatusRequest.newBuilder()
                .setWorkflowId(executionId)
                .setGraphId(graphId)
                .build());
        } catch (StatusRuntimeException e) {
            var causeStatus = e.getStatus();
            LOG.error("Cannot obtain graph status: { executionId: {}, graphId: {} }, error: {}",
                executionId, graphId, causeStatus.getDescription(), e);
            response.onError(causeStatus.withDescription("Cannot obtain graph status: " + causeStatus.getDescription())
                .asRuntimeException());
            return;
        }

        if (!graphStatus.hasStatus()) {
            LOG.error("Empty graph status for graph: { executionId: {}, graphId: {} }", executionId, graphId);
            response.onError(Status.INTERNAL.withDescription("Empty graph status for graph").asRuntimeException());
            return;
        }

        var graphStatusResponse = LWFS.GraphStatusResponse.newBuilder();

        switch (graphStatus.getStatus().getStatusCase()) {
            case WAITING -> graphStatusResponse.setWaiting(LWFS.GraphStatusResponse.Waiting.getDefaultInstance());
            case EXECUTING -> {
                var allTaskStatuses = graphStatus.getStatus().getExecuting().getExecutingTasksList();

                var executingTaskIds = new ArrayList<String>();
                var completedTaskIds = new ArrayList<String>();
                var waitingTaskIds = new ArrayList<String>();

                allTaskStatuses.forEach(status -> {
                    var taskId = status.getTaskDescriptionId();

                    if (!status.hasProgress()) {
                        LOG.error("Empty task status: { executionId: {}, graphId: {}, taskId: {} }", executionId,
                            graphId, taskId);
                        response.onError(Status.INTERNAL.withDescription("Empty status of task with ID: " + taskId)
                            .asRuntimeException());
                        return;
                    }

                    switch (status.getProgress().getStatusCase()) {
                        case EXECUTING -> executingTaskIds.add(taskId);
                        case SUCCESS, ERROR -> completedTaskIds.add(taskId);
                    }
                });

                graphStatusResponse.setExecuting(LWFS.GraphStatusResponse.Executing.newBuilder()
                    .setMessage("Graph status")
                    .addAllOperationsExecuting(executingTaskIds)
                    .addAllOperationsCompleted(completedTaskIds)
                    .addAllOperationsWaiting(waitingTaskIds));
            }
            case COMPLETED -> {
                var portalClient = portalClients.getGrpcClient(executionId);

                if (portalClient == null) {
                    response.onError(Status.INTERNAL
                        .withDescription("Error while creating portal channel")
                        .asException());
                    return;
                }

                LzyPortalApi.PortalStatusResponse status;
                GraphDao.GraphDescription desc;

                try {
                    desc = withRetries(LOG, () -> graphDao.get(graphId, executionId));

                    status = portalClient.status(LzyPortalApi.PortalStatusRequest.newBuilder()
                        .addAllSlotNames(desc.portalInputSlotNames())
                        .build());
                } catch (StatusRuntimeException e) {
                    LOG.error("Exception while getting status of portal", e);
                    response.onError(e);
                    return;
                } catch (Exception e) {
                    LOG.error("Exception while getting status of portal", e);
                    response.onError(Status.INTERNAL.asException());
                    return;
                }
                var allSynced = true;
                var hasFailed = false;

                for (var s : status.getSlotsList()) {
                    if (s.getSnapshotStatus() == SnapshotSlotStatus.NOT_IN_SNAPSHOT) {
                        continue;
                    }
                    if (s.getSnapshotStatus() != SnapshotSlotStatus.SYNCED) {
                        allSynced = false;
                    }
                    if (s.getSnapshotStatus() == SnapshotSlotStatus.FAILED) {
                        LOG.error(
                            "Portal slot <{}> of graph <{}> of execution <{}> is failed to sync data with storage",
                            s.getSlot().getName(), graphId, executionId
                        );
                        hasFailed = true;
                    }
                }

                if (hasFailed) {
                    graphStatusResponse.setFailed(LWFS.GraphStatusResponse.Failed.newBuilder()
                        .setDescription("Error while loading data to external storage")
                        .build());
                } else if (allSynced) {
                    graphStatusResponse.setCompleted(LWFS.GraphStatusResponse.Completed.getDefaultInstance());
                } else {
                    graphStatusResponse.setExecuting(LWFS.GraphStatusResponse.Executing.getDefaultInstance());
                }
            }
            case FAILED -> graphStatusResponse.setFailed(LWFS.GraphStatusResponse.Failed.newBuilder()
                .setDescription(graphStatus.getStatus().getFailed().getDescription()));
        }

        response.onNext(graphStatusResponse.build());
        response.onCompleted();
    }

    public void stopGraph(StopGraphRequest request, StreamObserver<StopGraphResponse> response) {
        var executionId = request.getExecutionId();
        var graphId = request.getGraphId();

        try {
            //noinspection ResultOfMethodCallIgnored
            graphExecutorClient.stop(GraphExecutorApi.GraphStopRequest.newBuilder()
                .setWorkflowId(executionId)
                .setGraphId(graphId)
                .build());
        } catch (StatusRuntimeException e) {
            var causeStatus = e.getStatus();
            LOG.error("Cannot stop graph: { executionId: {}, graphId: {} }, error: {}",
                executionId, graphId, causeStatus.getDescription());
            response.onError(causeStatus.withDescription("Cannot stop graph: " + causeStatus.getDescription())
                .asRuntimeException());
            return;
        }

        response.onNext(StopGraphResponse.getDefaultInstance());
        response.onCompleted();
    }

    private void setWorkflowName(GraphExecutionState state) {
        LOG.debug("Find workflow name of execution which graph belongs to...");

        try {
            var workflowInfo = withRetries(LOG, () -> workflowDao.findWorkflowBy(state.getExecutionId()));
            if (workflowInfo.workflowName() != null) {
                state.setWorkflowName(workflowInfo.workflowName());
            } else {
                state.fail(Status.NOT_FOUND, "Cannot obtain workflow name for execution");
            }
        } catch (Exception e) {
            state.fail(Status.INTERNAL, "Cannot obtain workflow name for execution: " + e.getMessage());
        }
    }
}
