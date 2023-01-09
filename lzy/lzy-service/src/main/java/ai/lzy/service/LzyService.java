package ai.lzy.service;

import ai.lzy.iam.grpc.context.AuthenticationContext;
import ai.lzy.longrunning.IdempotencyUtils;
import ai.lzy.longrunning.Operation;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.Storage;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.service.config.LzyServiceConfig;
import ai.lzy.service.data.dao.GraphDao;
import ai.lzy.service.data.dao.WorkflowDao;
import ai.lzy.service.data.storage.LzyServiceStorage;
import ai.lzy.service.gc.GarbageCollector;
import ai.lzy.service.graph.GraphExecutionService;
import ai.lzy.service.graph.GraphExecutionState;
import ai.lzy.service.graph.debug.InjectedFailures;
import ai.lzy.service.workflow.WorkflowService;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.workflow.LWF;
import ai.lzy.v1.workflow.LzyWorkflowServiceGrpc;
import com.google.common.annotations.VisibleForTesting;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;

import static ai.lzy.longrunning.IdempotencyUtils.handleIdempotencyKeyConflict;
import static ai.lzy.longrunning.IdempotencyUtils.loadExistingOpResult;
import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.v1.workflow.LWFS.*;

@Singleton
public class LzyService extends LzyWorkflowServiceGrpc.LzyWorkflowServiceImplBase {
    private static final Logger LOG = LogManager.getLogger(LzyService.class);

    public static final String APP = "LzyService";

    private final ExecutionFinalizer executionFinalizer;
    private final String instanceId;

    private final WorkflowService workflowService;
    private final GraphExecutionService graphExecutionService;

    private final ExecutorService workersPool;

    private final Storage storage;
    private final OperationDao operationDao;
    private final WorkflowDao workflowDao;
    private final GraphDao graphDao;

    public LzyService(WorkflowService workflowService, GraphExecutionService graphExecutionService,
                      LzyServiceStorage storage, GraphDao graphDao, WorkflowDao workflowDao,
                      @Named("LzyServiceServerExecutor") ExecutorService workersPool,
                      @Named("LzyServiceOperationDao") OperationDao operationDao,
                      ExecutionFinalizer executionFinalizer, GarbageCollector gc, LzyServiceConfig config)
    {
        this.executionFinalizer = executionFinalizer;
        this.instanceId = config.getInstanceId();
        this.workflowService = workflowService;
        this.graphExecutionService = graphExecutionService;
        this.workersPool = workersPool;
        this.operationDao = operationDao;
        this.workflowDao = workflowDao;
        this.graphDao = graphDao;
        this.storage = storage;

        gc.start();

        restartNotCompletedOps();
    }

    @VisibleForTesting
    public void testRestart() {
        restartNotCompletedOps();
    }

    private void restartNotCompletedOps() {
        try {
            var execGraphStates = graphDao.loadNotCompletedOpStates(instanceId, null);
            if (!execGraphStates.isEmpty()) {
                LOG.warn("Found {} not completed operations on lzy-service {}", execGraphStates.size(), instanceId);

                execGraphStates.forEach(state -> workersPool.submit(() -> graphExecutionService.executeGraph(state)));
            } else {
                LOG.info("Not completed lzy-service operations weren't found.");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void startExecution(StartExecutionRequest request, StreamObserver<StartExecutionResponse> responseObserver) {
        workflowService.startExecution(request, responseObserver);
    }

    @Override
    public void finishExecution(FinishExecutionRequest request, StreamObserver<LongRunning.Operation> response) {
        workflowService.finishExecution(request, response);
    }

    @Override
    public void executeGraph(ExecuteGraphRequest request, StreamObserver<ExecuteGraphResponse> responseObserver) {
        Operation.IdempotencyKey idempotencyKey = IdempotencyUtils.getIdempotencyKey(request);
        if (idempotencyKey != null &&
            loadExistingOpResult(operationDao, idempotencyKey, responseObserver, ExecuteGraphResponse.class,
                Duration.ofMillis(100), Duration.ofSeconds(5), LOG))
        {
            return;
        }

        String userId = AuthenticationContext.currentSubject().id();
        String executionId = request.getExecutionId();

        try {
            String[] wfNameAndUserId = withRetries(LOG, () -> workflowDao.findWorkflowBy(executionId));
            if (wfNameAndUserId == null || !Objects.equals(userId, wfNameAndUserId[1])) {
                LOG.error("Cannot find active execution of user: { executionId: {}, userId: {} }", executionId, userId);
                responseObserver.onError(Status.NOT_FOUND.withDescription("Cannot find active execution " +
                    "'%s' of user '%s'".formatted(executionId, userId)).asRuntimeException());
                return;
            }
        } catch (Exception e) {
            LOG.error("Cannot check that active execution of user exists: { executionId: {}, userId: " +
                "{}, error: {} } ", executionId, userId, e.getMessage());
            responseObserver.onError(Status.INTERNAL.withDescription("Cannot execute graph").asRuntimeException());
            return;
        }

        var op = Operation.create(userId, "Execute graph in execution: executionId='%s'".formatted(executionId),
            idempotencyKey, null);

        String parentGraphId = request.getGraph().getParentGraphId();
        String zone = request.getGraph().getZone();
        List<LWF.Operation> operations = request.getGraph().getOperationsList();
        List<LWF.DataDescription> descriptions = request.getGraph().getDataDescriptionsList();

        var state = new GraphExecutionState(executionId, op.id(), parentGraphId, userId, zone, descriptions,
            operations);

        try (var tx = TransactionHandle.create(storage)) {
            withRetries(LOG, () -> operationDao.create(op, tx));
            withRetries(LOG, () -> graphDao.put(state, instanceId, tx));

            tx.commit();
        } catch (Exception ex) {
            if (idempotencyKey != null && handleIdempotencyKeyConflict(idempotencyKey, ex, operationDao,
                responseObserver, ExecuteGraphResponse.class, Duration.ofMillis(100), Duration.ofSeconds(5), LOG))
            {
                return;
            }

            LOG.error("Cannot create execute graph operation for: { userId: {}, executionId: {}, error: {} }",
                userId, executionId, ex.getMessage(), ex);
            var status = Status.INTERNAL.withDescription(ex.getMessage());

            try {
                executionFinalizer.finishInDao(userId, executionId, status);
            } catch (Exception e) {
                LOG.warn("Execute graph fail. Cannot finish execution: { executionId: {}, error: {} }", executionId,
                    e.getMessage());
            }
            executionFinalizer.finalizeNow(executionId);

            responseObserver.onError(status.asException());
            return;
        }

        InjectedFailures.failExecuteGraph0();

        workersPool.submit(() -> {
            var completedOp = graphExecutionService.executeGraph(state);

            if (completedOp == null) {
                // emulates service shutdown
                responseObserver.onError(Status.DEADLINE_EXCEEDED.asRuntimeException());
                return;
            }

            try {
                var resp = completedOp.response();
                if (resp != null) {
                    responseObserver.onNext(resp.unpack(ExecuteGraphResponse.class));
                    responseObserver.onCompleted();
                } else {
                    var error = completedOp.error();
                    assert error != null;
                    responseObserver.onError(error.asRuntimeException());
                }
            } catch (Exception e) {
                var status = Status.INTERNAL.withDescription("Cannot execute graph");
                LOG.error("Cannot execute graph: {}", e.getMessage(), e);

                try {
                    executionFinalizer.finishInDao(userId, executionId, status);
                } catch (Exception ex) {
                    LOG.warn("Execute graph fail. Cannot finish execution: { executionId: {}, error: {} }", executionId,
                        e.getMessage());
                }
                executionFinalizer.finalizeNow(executionId);

                responseObserver.onError(status.asRuntimeException());
            }
        });
    }

    @Override
    public void graphStatus(GraphStatusRequest request, StreamObserver<GraphStatusResponse> responseObserver) {
        graphExecutionService.graphStatus(request, responseObserver);
    }

    @Override
    public void stopGraph(StopGraphRequest request, StreamObserver<StopGraphResponse> responseObserver) {
        graphExecutionService.stopGraph(request, responseObserver);
    }

    @Override
    public void readStdSlots(ReadStdSlotsRequest request, StreamObserver<ReadStdSlotsResponse> responseObserver) {
        workflowService.readStdSlots(request, responseObserver);
    }

    @Override
    public void getAvailablePools(GetAvailablePoolsRequest request,
                                  StreamObserver<GetAvailablePoolsResponse> responseObserver)
    {
        workflowService.getAvailablePools(request, responseObserver);
    }
}
