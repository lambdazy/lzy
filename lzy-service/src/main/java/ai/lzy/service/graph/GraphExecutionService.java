package ai.lzy.service.graph;

import ai.lzy.longrunning.IdempotencyUtils;
import ai.lzy.longrunning.Operation;
import ai.lzy.longrunning.dao.OperationDao;
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
import com.google.protobuf.Any;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import javax.annotation.Nullable;

import static ai.lzy.longrunning.IdempotencyUtils.handleIdempotencyKeyConflict;
import static ai.lzy.longrunning.IdempotencyUtils.loadExistingOpResult;
import static ai.lzy.model.db.DbHelper.defaultRetryPolicy;
import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.service.LzyService.APP;
import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;
import static ai.lzy.util.grpc.ProtoConverter.toProto;
import static ai.lzy.v1.graph.GraphExecutorGrpc.newBlockingStub;

@Singleton
public class GraphExecutionService {
    private static final Logger LOG = LogManager.getLogger(GraphExecutionService.class);

    private final RenewableJwt internalUserCredentials;

    private final WorkflowDao workflowDao;
    private final GraphDao graphDao;
    private final OperationDao operationDao;

    private final GraphExecutorGrpc.GraphExecutorBlockingStub graphExecutorClient;

    private final GraphValidator validator;
    private final GraphBuilder builder;

    private final Map<String, ManagedChannel> portalChannelForExecution = new ConcurrentHashMap<>();

    public GraphExecutionService(GraphDao graphDao, WorkflowDao workflowDao, ExecutionDao executionDao,
                                 @Named("LzyServiceOperationDao") OperationDao operationDao,
                                 @Named("LzyServiceIamToken") RenewableJwt internalUserCredentials,
                                 @Named("AllocatorServiceChannel") ManagedChannel allocatorChannel,
                                 @Named("ChannelManagerServiceChannel") ManagedChannel channelManagerChannel,
                                 @Named("GraphExecutorServiceChannel") ManagedChannel graphExecutorChannel)
    {
        this.internalUserCredentials = internalUserCredentials;

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

        this.builder = new GraphBuilder(workflowDao, executionDao, channelManagerClient);
    }

    public void executeGraph(ExecuteGraphRequest request, StreamObserver<ExecuteGraphResponse> responseObserver) {
        var idempotencyKey = IdempotencyUtils.getIdempotencyKey(request);
        if (idempotencyKey != null &&
            loadExistingOpResult(operationDao, idempotencyKey, responseObserver, ExecuteGraphResponse.class,
                Duration.ofMillis(100), Duration.ofSeconds(5), LOG))
        {
            return;
        }

        var executionId = request.getExecutionId();
        var graphExecutionState = new GraphExecutionState(executionId);

        setWorkflowInfo(graphExecutionState);

        Consumer<Status> replyError = (status) -> {
            LOG.error("[executeGraph], fail: status={}, msg={}.", status,
                status.getDescription() + ", graphExecutionState: " + graphExecutionState);
            responseObserver.onError(status.asRuntimeException());
        };

        if (graphExecutionState.isInvalid()) {
            replyError.accept(graphExecutionState.getErrorStatus());
            return;
        }

        var userId = graphExecutionState.getUserId();
        var workflowName = graphExecutionState.getWorkflowName();

        LOG.info("Create execute graph operation for: { userId: {}, workflowName: {}, executionId: {} }", userId,
            workflowName, executionId);

        final var op = Operation.create(
            userId,
            "Execute graph: executionId='%s', workflowName='%s'".formatted(executionId, workflowName),
            idempotencyKey,
            null);

        try {
            withRetries(LOG, () -> operationDao.create(op, null));
        } catch (Exception ex) {
            if (idempotencyKey != null && handleIdempotencyKeyConflict(idempotencyKey, ex, operationDao,
                responseObserver, ExecuteGraphResponse.class, Duration.ofMillis(100), Duration.ofSeconds(5), LOG))
            {
                return;
            }

            LOG.error("Cannot create execute graph operation for: { userId: {}, workflowName: {}, executionId: {} }" +
                ", error: {}", userId, workflowName, executionId, ex.getMessage(), ex);
            responseObserver.onError(Status.INTERNAL.withDescription(ex.getMessage()).asException());
            return;
        }

        LWF.Graph graph = request.getGraph();

        graphExecutionState.setZone(graph.getZone());
        graphExecutionState.setOperations(graph.getOperationsList());
        graphExecutionState.setDescriptions(graph.getDataDescriptionsList());

        LOG.debug("[executeGraph], validate dataflow graph, current state: " + graphExecutionState);

        validator.validate(graphExecutionState);

        if (graphExecutionState.isInvalid()) {
            operationDao.failOperation(op.id(), toProto(graphExecutionState.getErrorStatus()), LOG);
            replyError.accept(graphExecutionState.getErrorStatus());
            return;
        }

        LOG.info("[executeGraph], dataflow graph built and validated: " +
            graphExecutionState.getDataFlowGraph().toString());

        LOG.debug("[executeGraph], building execution graph, current state: " + graphExecutionState);

        ManagedChannel portalChannel = getOrCreatePortalChannel(graphExecutionState.getExecutionId());

        if (portalChannel == null) {
            var status = Status.INTERNAL.withDescription("Cannot build execution graph");
            operationDao.failOperation(op.id(), toProto(status), LOG);
            replyError.accept(status);
            return;
        }

        var portalClient = newBlockingClient(LzyPortalGrpc.newBlockingStub(portalChannel),
            APP, () -> internalUserCredentials.get().token());

        builder.build(graphExecutionState, portalClient);

        if (graphExecutionState.isInvalid()) {
            operationDao.failOperation(op.id(), toProto(graphExecutionState.getErrorStatus()), LOG);
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
            operationDao.failOperation(op.id(), toProto(causeStatus), LOG);
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
            var status = Status.INTERNAL.withDescription("Error while graph execution");
            operationDao.failOperation(op.id(), toProto(status), LOG);
            replyError.accept(status);
            return;
        }

        LOG.debug("[executeGraph], graph successfully executed, current state: " + graphExecutionState);

        var response = ExecuteGraphResponse.newBuilder()
            .setGraphId(executeResponse.getStatus().getGraphId())
            .build();
        var packedResponse = Any.pack(response);

        try {
            withRetries(LOG, () -> operationDao.updateResponse(op.id(), packedResponse.toByteArray(), null));
        } catch (Exception e) {
            LOG.error("Error while executing transaction: {}", e.getMessage(), e);
            var errorStatus = Status.INTERNAL.withDescription("Error while execute graph: " + e.getMessage());

            operationDao.failOperation(op.id(), toProto(errorStatus), LOG);

            responseObserver.onError(errorStatus.asRuntimeException());
        }

        responseObserver.onNext(response);
        responseObserver.onCompleted();
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
