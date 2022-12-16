package ai.lzy.service.graph;

import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.NotFoundException;
import ai.lzy.service.data.dao.GraphDao;
import ai.lzy.service.data.dao.GraphDao.GraphDescription;
import ai.lzy.service.data.dao.WorkflowDao;
import ai.lzy.v1.graph.GraphExecutorApi;
import ai.lzy.v1.workflow.LWFS.ExecuteGraphResponse;
import com.google.protobuf.Any;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;

import static ai.lzy.model.db.DbHelper.defaultRetryPolicy;
import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.util.grpc.GrpcUtils.withIdempotencyKey;
import static ai.lzy.util.grpc.ProtoConverter.toProto;


public class ExecuteGraphAction implements Runnable {
    private static final Logger LOG = LogManager.getLogger(ExecuteGraphAction.class);

    private final GraphExecutionState state;
    private final GraphValidator validator;
    private final GraphBuilder builder;

    private final OperationDao operationDao;
    private final GraphDao graphDao;
    private final GraphExecutionService ownerInstance;

    private final WorkflowDao workflowDao;
    private final StreamObserver<ExecuteGraphResponse> responseObserver;

    public ExecuteGraphAction(GraphExecutionService ownerInstance, GraphExecutionState initialState,
                              GraphValidator validator, GraphBuilder builder,
                              OperationDao operationDao, GraphDao graphDao, WorkflowDao workflowDao,
                              StreamObserver<ExecuteGraphResponse> responseObserver)
    {
        this.state = initialState;
        this.validator = validator;
        this.builder = builder;
        this.operationDao = operationDao;
        this.graphDao = graphDao;
        this.ownerInstance = ownerInstance;
        this.workflowDao = workflowDao;
        this.responseObserver = responseObserver;
    }

    @Override
    public void run() {
        LOG.info("Start processing execute graph operation: { operationId: {} }", state.getOpId());

        if (state.getWorkflowName() == null) {
            LOG.debug("Find workflow name of execution which graph belongs to...");
            setWorkflowName(state);
        }

        if (state.isInvalid()) {
            operationDao.failOperation(state.getOpId(), toProto(state.getErrorStatus()), LOG);
            // todo: fail an entire execution. Then gc considers the execution as trash
            responseObserver.onError(state.getErrorStatus().asRuntimeException());
            return;
        }

        if (state.getDataFlowGraph() == null || state.getZone() == null) {
            LOG.debug("[executeGraph], validate dataflow graph, current state: " + state);
            validator.validate(state);
        }

        if (state.isInvalid()) {
            operationDao.failOperation(state.getOpId(), toProto(state.getErrorStatus()), LOG);
            updateExecutionStatus(state.getWorkflowName(), state.getUserId(), state.getExecutionId(),
                state.getErrorStatus());
            responseObserver.onError(state.getErrorStatus().asRuntimeException());
            return;
        }

        LOG.info("[executeGraph], dataflow graph built and validated: " +
            state.getDataFlowGraph().toString());

        LOG.debug("[executeGraph], building execution graph, current state: " + state);

        var portalClient = ownerInstance.getPortalClient(state.getExecutionId());

        if (portalClient == null) {
            var status = Status.INTERNAL.withDescription("Cannot build execution graph");
            operationDao.failOperation(state.getOpId(), toProto(status), LOG);
            updateExecutionStatus(state.getWorkflowName(), state.getUserId(), state.getExecutionId(), status);
            responseObserver.onError(status.asRuntimeException());
            return;
        }

        if (state.getTasks() == null) {
            builder.build(state, portalClient);
        }

        if (state.isInvalid()) {
            operationDao.failOperation(state.getOpId(), toProto(state.getErrorStatus()), LOG);
            updateExecutionStatus(state.getWorkflowName(), state.getUserId(), state.getExecutionId(),
                state.getErrorStatus());
            responseObserver.onError(state.getErrorStatus().asRuntimeException());
            return;
        }

        if (state.getGraphId() == null) {
            LOG.debug("[executeGraph], executing graph, current state: " + state);

            // todo: generate idempotency key and save it to state then use the key to call graphExecutor.execute(...)
            var idempotencyKey = UUID.randomUUID().toString();
            var graphExecutorClient = withIdempotencyKey(ownerInstance.getGraphExecutorClient(), idempotencyKey);

            GraphExecutorApi.GraphExecuteResponse executeResponse;
            try {
                executeResponse = graphExecutorClient.execute(
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
                operationDao.failOperation(state.getOpId(), toProto(causeStatus), LOG);
                updateExecutionStatus(state.getWorkflowName(), state.getUserId(), state.getExecutionId(), causeStatus);
                responseObserver.onError(causeStatus.asRuntimeException());
                return;
            }

            state.setGraphId(executeResponse.getStatus().getGraphId());
        }

        try {
            withRetries(defaultRetryPolicy(), LOG, () -> graphDao.save(
                new GraphDescription(state.getGraphId(), state.getExecutionId(), state.getPortalInputSlots())));
        } catch (Exception e) {
            LOG.error("Cannot save portal slots", e);
            var status = Status.INTERNAL.withDescription("Error while graph execution");
            operationDao.failOperation(state.getOpId(), toProto(status), LOG);
            responseObserver.onError(status.asRuntimeException());
            return;
        }

        LOG.debug("[executeGraph], graph successfully executed, current state: " + state);

        var response = ExecuteGraphResponse.newBuilder()
            .setGraphId(state.getGraphId())
            .build();
        var packedResponse = Any.pack(response);

        try {
            withRetries(LOG, () -> operationDao.updateResponse(state.getOpId(), packedResponse.toByteArray(), null));
        } catch (Exception e) {
            LOG.error("Error while executing transaction: {}", e.getMessage(), e);
            var errorStatus = Status.INTERNAL.withDescription("Error while execute graph: " + e.getMessage());

            operationDao.failOperation(state.getOpId(), toProto(errorStatus), LOG);
            responseObserver.onError(errorStatus.asRuntimeException());
            return;
        }

        responseObserver.onNext(response);
        responseObserver.onCompleted();
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
                try (var transaction = TransactionHandle.create(ownerInstance.getStorage())) {
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
