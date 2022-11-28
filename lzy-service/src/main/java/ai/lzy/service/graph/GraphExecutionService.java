package ai.lzy.service.graph;

import ai.lzy.model.db.exceptions.NotFoundException;
import ai.lzy.service.data.dao.ExecutionDao;
import ai.lzy.service.data.dao.GraphDao;
import ai.lzy.service.data.dao.WorkflowDao;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.v1.VmPoolServiceGrpc;
import ai.lzy.v1.channel.LzyChannelManagerPrivateGrpc;
import ai.lzy.v1.graph.GraphExecutorApi;
import ai.lzy.v1.graph.GraphExecutorGrpc;
import ai.lzy.v1.portal.LzyPortalApi;
import ai.lzy.v1.portal.LzyPortalApi.PortalSlotStatus.SnapshotSlotStatus;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import javax.annotation.Nullable;

import static ai.lzy.model.db.DbHelper.defaultRetryPolicy;
import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.service.LzyService.APP;
import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;

public class GraphExecutionService {
    private static final Logger LOG = LogManager.getLogger(GraphExecutionService.class);

    private final RenewableJwt internalUserCredentials;

    private final WorkflowDao workflowDao;
    private final GraphDao graphDao;

    private final GraphExecutorGrpc.GraphExecutorBlockingStub graphExecutorClient;

    private final GraphValidator validator;
    private final GraphBuilder builder;

    private final Map<String, ManagedChannel> portalChannelForExecution = new ConcurrentHashMap<>();

    public GraphExecutionService(RenewableJwt internalUserCredentials,
                                 WorkflowDao workflowDao, GraphDao graphDao, ExecutionDao executionDao,
                                 VmPoolServiceGrpc.VmPoolServiceBlockingStub vmPoolClient,
                                 GraphExecutorGrpc.GraphExecutorBlockingStub graphExecutorClient,
                                 LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateBlockingStub channelManagerClient)
    {
        this.internalUserCredentials = internalUserCredentials;

        this.workflowDao = workflowDao;
        this.graphDao = graphDao;

        this.graphExecutorClient = graphExecutorClient;

        this.validator = new GraphValidator(executionDao, vmPoolClient);
        this.builder = new GraphBuilder(workflowDao, executionDao, channelManagerClient);
    }

    public void executeGraph(ExecuteGraphRequest request, StreamObserver<ExecuteGraphResponse> response) {
        var executionId = request.getExecutionId();
        var graphExecutionState = new GraphExecutionState(executionId);

        LOG.debug("[executeGraph], validate dataflow graph, current state: " + graphExecutionState);

        Consumer<Status> replyError = (status) -> {
            LOG.error("[executeGraph], fail: status={}, msg={}.", status,
                status.getDescription() + ", graphExecutionState: " + graphExecutionState);
            response.onError(status.asRuntimeException());
        };

        setWorkflowInfo(graphExecutionState);

        if (graphExecutionState.isInvalid()) {
            replyError.accept(graphExecutionState.getErrorStatus());
            return;
        }

        LWF.Graph graph = request.getGraph();

        graphExecutionState.setZone(graph.getZone());
        graphExecutionState.setOperations(graph.getOperationsList());
        graphExecutionState.setDescriptions(graph.getDataDescriptionsList());

        validator.validate(graphExecutionState);

        if (graphExecutionState.isInvalid()) {
            replyError.accept(graphExecutionState.getErrorStatus());
            return;
        }

        LOG.info("[executeGraph], dataflow graph built and validated: " +
            graphExecutionState.getDataFlowGraph().toString());

        LOG.debug("[executeGraph], building execution graph, current state: " + graphExecutionState);

        ManagedChannel portalChannel = getOrCreatePortalChannel(executionId);

        if (portalChannel == null) {
            replyError.accept(Status.INTERNAL.withDescription("Cannot build execution graph"));
            return;
        }

        var portalClient = newBlockingClient(LzyPortalGrpc.newBlockingStub(portalChannel),
            APP, () -> internalUserCredentials.get().token());

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
                .setUserId(graphExecutionState.getUserId())
                .setParentGraphId(graph.getParentGraphId())
                .addAllTasks(graphExecutionState.getTasks())
                .addAllChannels(graphExecutionState.getChannels())
                .build());
        } catch (StatusRuntimeException e) {
            var causeStatus = e.getStatus();
            replyError.accept(causeStatus.withDescription("Cannot execute graph: " + causeStatus.getDescription()));
            return;
        }


        try {
            withRetries(
                defaultRetryPolicy(), LOG, () -> graphDao.save(new GraphDao.GraphDescription(
                    executeResponse.getStatus().getGraphId(), executionId, graphExecutionState.getPortalInputSlots()
            )));
        } catch (Exception e) {
            LOG.error("Cannot save portal slots", e);
            replyError.accept(Status.INTERNAL);
            return;
        }

        LOG.debug("[executeGraph], graph successfully executed, current state: " + graphExecutionState);

        response.onNext(ExecuteGraphResponse.newBuilder().setGraphId(executeResponse.getStatus().getGraphId()).build());
        response.onCompleted();
    }

    @Nullable
    private ManagedChannel getOrCreatePortalChannel(String executionId) {
        return portalChannelForExecution.computeIfAbsent(executionId, exId -> {
            String portalAddress;
            try {
                portalAddress = withRetries(LOG, () -> workflowDao.getPortalAddress(exId));
            } catch (Exception e) {
                LOG.error("Error while getting portal address: ", e);
                return null;
            }
            return newGrpcChannel(portalAddress, LzyPortalGrpc.SERVICE_NAME);
        });
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
            case COMPLETED -> {
                var portalChannel = getOrCreatePortalChannel(executionId);
                if (portalChannel == null) {
                    response.onError(Status.INTERNAL
                        .withDescription("Error while creating portal channel")
                        .asException());
                    return;
                }

                var portalClient = newBlockingClient(LzyPortalGrpc.newBlockingStub(portalChannel),
                    APP, () -> internalUserCredentials.get().token());

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

                for (var s: status.getSlotsList()) {
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


    private void setWorkflowInfo(GraphExecutionState state) {
        try {
            state.setWorkflowName(withRetries(LOG, () -> workflowDao.getWorkflowName(state.getExecutionId())));
        } catch (NotFoundException e) {
            state.fail(Status.NOT_FOUND, "Cannot obtain workflow name for execution: " + e.getMessage());
            return;
        } catch (Exception e) {
            state.fail(Status.INTERNAL, "Cannot obtain workflow name for execution: " + e.getMessage());
            return;
        }

        try {
            state.setUserId(withRetries(LOG, () -> workflowDao.getUserId(state.getExecutionId())));
        } catch (NotFoundException e) {
            state.fail(Status.NOT_FOUND, "Cannot obtain userId for execution: " + e.getMessage());
        } catch (Exception e) {
            state.fail(Status.INTERNAL, "Cannot obtain userId for execution: " + e.getMessage());
        }
    }
}
