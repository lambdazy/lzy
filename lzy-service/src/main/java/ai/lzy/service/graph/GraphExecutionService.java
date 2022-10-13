package ai.lzy.service.graph;

import ai.lzy.model.db.exceptions.NotFoundException;
import ai.lzy.service.data.dao.ExecutionDao;
import ai.lzy.service.data.dao.WorkflowDao;
import ai.lzy.util.auth.credentials.JwtCredentials;
import ai.lzy.v1.VmPoolServiceGrpc;
import ai.lzy.v1.channel.LzyChannelManagerPrivateGrpc;
import ai.lzy.v1.graph.GraphExecutorApi;
import ai.lzy.v1.graph.GraphExecutorGrpc;
import ai.lzy.v1.portal.LzyPortalGrpc;
import ai.lzy.v1.workflow.LWF;
import ai.lzy.v1.workflow.LWFS;
import ai.lzy.v1.workflow.LWFS.ExecuteGraphRequest;
import ai.lzy.v1.workflow.LWFS.ExecuteGraphResponse;
import ai.lzy.v1.workflow.LWFS.StopGraphRequest;
import ai.lzy.v1.workflow.LWFS.StopGraphResponse;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.service.LzyService.APP;
import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;

public class GraphExecutionService {
    private static final Logger LOG = LogManager.getLogger(GraphExecutionService.class);

    private final JwtCredentials internalUserCredentials;

    private final WorkflowDao workflowDao;

    private final GraphExecutorGrpc.GraphExecutorBlockingStub graphExecutorClient;

    private final GraphValidator validator;
    private final GraphBuilder builder;

    private final Map<String, ManagedChannel> portalChannelForExecution = new ConcurrentHashMap<>();

    public GraphExecutionService(JwtCredentials internalUserCredentials,
                                 WorkflowDao workflowDao, ExecutionDao executionDao,
                                 VmPoolServiceGrpc.VmPoolServiceBlockingStub vmPoolClient,
                                 GraphExecutorGrpc.GraphExecutorBlockingStub graphExecutorClient,
                                 LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateBlockingStub channelManagerClient)
    {
        this.internalUserCredentials = internalUserCredentials;

        this.workflowDao = workflowDao;

        this.graphExecutorClient = graphExecutorClient;

        this.validator = new GraphValidator(executionDao, vmPoolClient);
        this.builder = new GraphBuilder(workflowDao, executionDao, channelManagerClient);
    }

    public void executeGraph(ExecuteGraphRequest request, StreamObserver<ExecuteGraphResponse> response) {
        var executionId = request.getExecutionId();
        var graphExecutionState = new GraphExecutionState(executionId);

        LOG.debug("[executeGraph], validate graph, current state: " + graphExecutionState);

        Consumer<Status> replyError = (status) -> {
            LOG.error("[executeGraph], fail: status={}, msg={}.", status,
                status.getDescription() + ", graphExecutionState: " + graphExecutionState);
            response.onError(status.asRuntimeException());
        };

        setWorkflowName(graphExecutionState);

        if (graphExecutionState.isInvalid()) {
            replyError.accept(graphExecutionState.getErrorStatus());
            return;
        }

        LWF.Graph graph = request.getGraph();
        graphExecutionState.setOperations(graph.getOperationsList());
        graphExecutionState.setDescriptions(graph.getDataDescriptionsList());

        validator.validate(graphExecutionState, graph);

        LOG.debug("[executeGraph], building graph, current state: " + graphExecutionState);

        if (graphExecutionState.isInvalid()) {
            replyError.accept(graphExecutionState.getErrorStatus());
            return;
        }

        ManagedChannel portalChannel;
        try {
            portalChannel = portalChannelForExecution.computeIfAbsent(executionId, exId -> {
                String portalAddress;
                try {
                    portalAddress = withRetries(LOG, () -> workflowDao.getPortalAddress(exId));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return newGrpcChannel(portalAddress, LzyPortalGrpc.SERVICE_NAME);
            });
        } catch (RuntimeException e) {
            var cause = Objects.nonNull(e.getCause()) ? e.getCause() : e;
            replyError.accept(Status.INTERNAL.withDescription("Cannot build graph: " +
                cause.getMessage()));
            return;
        }

        var portalClient = newBlockingClient(LzyPortalGrpc.newBlockingStub(portalChannel),
            APP, internalUserCredentials::token);

        builder.build(graphExecutionState, portalClient);

        if (graphExecutionState.isInvalid()) {
            replyError.accept(graphExecutionState.getErrorStatus());
            return;
        }

        LOG.debug("[executeGraph], executing graph, current state: " + graphExecutionState);

        GraphExecutorApi.GraphExecuteResponse executeResponse;
        try {
            executeResponse = graphExecutorClient.execute(GraphExecutorApi.GraphExecuteRequest.newBuilder()
                .setWorkflowId(executionId)
                .setWorkflowName(graphExecutionState.getWorkflowName())
                .setParentGraphId(graph.getParentGraphId())
                .addAllTasks(graphExecutionState.getTasks())
                .addAllChannels(graphExecutionState.getChannels())
                .build());
        } catch (StatusRuntimeException e) {
            var causeStatus = e.getStatus();
            replyError.accept(causeStatus.withDescription("Cannot execute graph: " + causeStatus.getDescription()));
            return;
        }

        LOG.debug("[executeGraph], graph successfully executed, current state: " + graphExecutionState);

        response.onNext(ExecuteGraphResponse.newBuilder().setGraphId(executeResponse.getStatus().getGraphId()).build());
        response.onCompleted();
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
                executionId, graphId, causeStatus.getDescription());
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
                        case QUEUE -> waitingTaskIds.add(taskId);
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
            case COMPLETED -> graphStatusResponse.setCompleted(LWFS.GraphStatusResponse.Completed.getDefaultInstance());
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
        try {
            state.setWorkflowName(withRetries(LOG, () -> workflowDao.getWorkflowName(state.getExecutionId())));
        } catch (NotFoundException e) {
            state.onError(Status.NOT_FOUND, "Cannot obtain workflow name for execution: " + e.getMessage());
        } catch (Exception e) {
            state.onError(Status.INTERNAL, "Cannot obtain workflow name for execution: " + e.getMessage());
        }
    }
}
