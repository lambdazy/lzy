package ai.lzy.service.graph;

import ai.lzy.longrunning.Operation;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.Storage;
import ai.lzy.model.db.TransactionHandle;
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
import ai.lzy.v1.workflow.LWFS;
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

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;

import static ai.lzy.model.db.DbHelper.defaultRetryPolicy;
import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.service.LzyService.APP;
import static ai.lzy.util.grpc.GrpcUtils.*;
import static ai.lzy.util.grpc.ProtoConverter.toProto;
import static ai.lzy.v1.graph.GraphExecutorGrpc.newBlockingStub;

@Singleton
public class GraphExecutionService {
    private static final Logger LOG = LogManager.getLogger(GraphExecutionService.class);

    private final Storage storage;
    private final WorkflowDao workflowDao;
    private final GraphDao graphDao;
    private final OperationDao operationDao;

    private final GraphValidator validator;
    private final GraphBuilder builder;

    private final RenewableJwt internalUserCredentials;
    private final GraphExecutorGrpc.GraphExecutorBlockingStub graphExecutorClient;

    private final Map<String, ManagedChannel> executionId2portalChannel = new ConcurrentHashMap<>();

    public GraphExecutionService(GraphDao graphDao, WorkflowDao workflowDao, ExecutionDao executionDao,
                                 @Named("LzyServiceStorage") Storage storage,
                                 @Named("LzyServiceOperationDao") OperationDao operationDao,
                                 @Named("LzyServiceIamToken") RenewableJwt internalUserCredentials,
                                 @Named("AllocatorServiceChannel") ManagedChannel allocatorChannel,
                                 @Named("ChannelManagerServiceChannel") ManagedChannel channelManagerChannel,
                                 @Named("GraphExecutorServiceChannel") ManagedChannel graphExecutorChannel)
    {
        this.internalUserCredentials = internalUserCredentials;

        this.storage = storage;
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

    public Operation executeGraph(GraphExecutionState state) {
        LOG.info("Start processing execute graph operation: { operationId: {} }", state.getOpId());

        LOG.debug("Find workflow name of execution which graph belongs to...");

        // todo: test what if fails here

        setWorkflowName(state);

        if (state.isInvalid()) {
            updateExecutionStatus(state.getWorkflowName(), state.getUserId(), state.getExecutionId(),
                state.getErrorStatus());
            return operationDao.failOperation(state.getOpId(), toProto(state.getErrorStatus()), LOG);
        }

        try {
            if (state.getDataFlowGraph() == null || state.getZone() == null) {
                LOG.debug("Validate dataflow graph, current state: " + state);
                validator.validate(state);

                // todo: test what if fails here

                withRetries(LOG, () -> graphDao.update(state, null));
            }

            if (state.isInvalid()) {
                updateExecutionStatus(state.getWorkflowName(), state.getUserId(), state.getExecutionId(),
                    state.getErrorStatus());
                return operationDao.failOperation(state.getOpId(), toProto(state.getErrorStatus()), LOG);
            }

            LOG.info("Dataflow graph built and validated, building execution graph, current state:" + state);

            var portalClient = getPortalClient(state.getExecutionId());

            if (portalClient == null) {
                LOG.error("Cannot get portal client while creating portal slots for current graph: " + state);
                var status = Status.INTERNAL.withDescription("Cannot build execution graph");
                updateExecutionStatus(state.getWorkflowName(), state.getUserId(), state.getExecutionId(), status);
                return operationDao.failOperation(state.getOpId(), toProto(status), LOG);
            }

            // todo: test what if fails here

            if (state.getTasks() == null) {
                LOG.debug("Building graph, current state: " + state);
                builder.build(state, portalClient);

                // todo: test what if fails here

                withRetries(LOG, () -> graphDao.update(state, null));
            }

            if (state.isInvalid()) {
                updateExecutionStatus(state.getWorkflowName(), state.getUserId(), state.getExecutionId(),
                    state.getErrorStatus());
                return operationDao.failOperation(state.getOpId(), toProto(state.getErrorStatus()), LOG);
            }

            LOG.info("Graph successfully built: " + state);

            if (state.getGraphId() == null) {
                LOG.debug("Send execute graph request to graph execution service, current state: " + state);

                // todo: test what if fails here

                String idempotencyKey = state.getOrGenerateIdempotencyKey();

                withRetries(LOG, () -> graphDao.update(state, null));

                var idempotentGraphExecClient = withIdempotencyKey(graphExecutorClient, idempotencyKey);

                GraphExecutorApi.GraphExecuteResponse executeResponse;
                try {
                    executeResponse = idempotentGraphExecClient.execute(
                        GraphExecutorApi.GraphExecuteRequest.newBuilder()
                            .setWorkflowId(state.getExecutionId())
                            .setWorkflowName(state.getWorkflowName())
                            .setUserId(state.getUserId())
                            .setParentGraphId(state.getParentGraphId())
                            .addAllTasks(state.getTasks())
                            .addAllChannels(state.getChannels())
                            .build());
                } catch (StatusRuntimeException e) {
                    var causeStatus = e.getStatus();
                    updateExecutionStatus(state.getWorkflowName(), state.getUserId(), state.getExecutionId(),
                        causeStatus);
                    return operationDao.failOperation(state.getOpId(), toProto(causeStatus), LOG);
                }

                state.setGraphId(executeResponse.getStatus().getGraphId());

                // todo: test what if fails here

                withRetries(LOG, () -> graphDao.update(state, null));
            }

            // todo: test what if fails here

            try {
                withRetries(defaultRetryPolicy(), LOG, () -> graphDao.save(new GraphDao.GraphDescription(
                    state.getGraphId(), state.getExecutionId(), state.getPortalInputSlots())));
            } catch (Exception e) {
                LOG.error("Cannot save portal slots", e);

                var stopResponse = graphExecutorClient.stop(
                    GraphExecutorApi.GraphStopRequest.newBuilder().setGraphId(state.getGraphId()).build());

                var status = Status.INTERNAL.withDescription("Error while graph execution");
                updateExecutionStatus(state.getWorkflowName(), state.getUserId(), state.getExecutionId(), status);
                return operationDao.failOperation(state.getOpId(), toProto(status), LOG);
            }

            // todo: test what if fails here

            LOG.info("Graph successfully executed, current state: " + state);

            var response = ExecuteGraphResponse.newBuilder()
                .setGraphId(state.getGraphId())
                .build();
            var packed = Any.pack(response);

            try {
                return withRetries(LOG, () -> operationDao.updateResponse(state.getOpId(), packed.toByteArray(), null));
            } catch (Exception e) {
                LOG.error("Error while executing transaction: {}", e.getMessage(), e);

                var stopResponse = graphExecutorClient.stop(
                    GraphExecutorApi.GraphStopRequest.newBuilder().setGraphId(state.getGraphId()).build());

                var errorStatus = Status.INTERNAL.withDescription("Error while execute graph: " + e.getMessage());
                updateExecutionStatus(state.getWorkflowName(), state.getUserId(), state.getExecutionId(), errorStatus);
                return operationDao.failOperation(state.getOpId(), toProto(errorStatus), LOG);
            }
        } catch (Exception e) {
            var errorStatus = Status.INTERNAL.withDescription("Error while execute graph: " + e.getMessage());
            updateExecutionStatus(state.getWorkflowName(), state.getUserId(), state.getExecutionId(), errorStatus);
            return operationDao.failOperation(state.getOpId(), toProto(errorStatus), LOG);
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
                var portalClient = getPortalClient(executionId);

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

    @Nullable
    public LzyPortalGrpc.LzyPortalBlockingStub getPortalClient(String executionId) {
        String address;
        try {
            address = withRetries(LOG, () -> workflowDao.getPortalAddress(executionId));
        } catch (Exception e) {
            LOG.error("Cannot obtain portal address { executionId: {}, error: {} } ", executionId, e.getMessage(), e);
            return null;
        }

        var grpcChannel = executionId2portalChannel.computeIfAbsent(executionId, exId ->
            newGrpcChannel(address, LzyPortalGrpc.SERVICE_NAME));

        return Objects.nonNull(grpcChannel) ? newBlockingClient(
            LzyPortalGrpc.newBlockingStub(grpcChannel), APP, () -> internalUserCredentials.get().token()) : null;
    }

    private void setWorkflowName(GraphExecutionState state) {
        try {
            state.setWorkflowName(withRetries(LOG, () -> workflowDao.getWorkflowName(state.getExecutionId())));
        } catch (NotFoundException e) {
            state.fail(Status.NOT_FOUND, "Cannot obtain workflow name for execution: " + e.getMessage());
        } catch (Exception e) {
            state.fail(Status.INTERNAL, "Cannot obtain workflow name for execution: " + e.getMessage());
        }
    }

    private void updateExecutionStatus(String workflowName, String userId, String executionId, Status status) {
        try {
            withRetries(defaultRetryPolicy(), LOG, () -> {
                try (var transaction = TransactionHandle.create(storage)) {
                    workflowDao.updateFinishData(workflowName, executionId,
                        Timestamp.from(Instant.now()), status.getDescription(), status.getCode().value(), transaction);
                    workflowDao.updateActiveExecution(userId, workflowName, executionId, null, transaction);

                    transaction.commit();
                }
            });
        } catch (Exception e) {
            LOG.error("[executeGraph] Got Exception during saving error status: " + e.getMessage(), e);
        }
    }
}
