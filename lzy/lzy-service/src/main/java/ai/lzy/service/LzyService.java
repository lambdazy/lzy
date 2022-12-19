package ai.lzy.service;

import ai.lzy.iam.grpc.context.AuthenticationContext;
import ai.lzy.longrunning.IdempotencyUtils;
import ai.lzy.longrunning.Operation;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.Storage;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.service.data.dao.GraphDao;
import ai.lzy.service.data.dao.WorkflowDao;
import ai.lzy.service.data.storage.LzyServiceStorage;
import ai.lzy.service.gc.GarbageCollector;
import ai.lzy.service.graph.GraphExecutionService;
import ai.lzy.service.graph.GraphExecutionState;
import ai.lzy.service.workflow.WorkflowService;
import ai.lzy.v1.workflow.LWF;
import ai.lzy.v1.workflow.LzyWorkflowServiceGrpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static ai.lzy.longrunning.IdempotencyUtils.handleIdempotencyKeyConflict;
import static ai.lzy.longrunning.IdempotencyUtils.loadExistingOpResult;
import static ai.lzy.model.db.DbHelper.defaultRetryPolicy;
import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.v1.workflow.LWFS.*;

@Singleton
public class LzyService extends LzyWorkflowServiceGrpc.LzyWorkflowServiceImplBase {
    private static final Logger LOG = LogManager.getLogger(LzyService.class);

    public static final String APP = "LzyService";

    private final WorkflowService workflowService;
    private final GraphExecutionService graphExecutionService;

    private final ExecutorService workersPool;
    private final GarbageCollector garbageCollector;

    private final Storage storage;
    private final OperationDao operationDao;
    private final GraphDao graphDao;
    private final WorkflowDao workflowDao;

    public LzyService(WorkflowService workflowService, GraphExecutionService graphExecutionService,
                      @Named("LzyServiceServerExecutor") ExecutorService workersPool,
                      @Named("LzyServiceOperationDao") OperationDao operationDao,
                      GraphDao graphDao, WorkflowDao workflowDao, LzyServiceStorage storage, GarbageCollector gc)
    {
        this.workflowService = workflowService;
        this.graphExecutionService = graphExecutionService;
        this.workersPool = workersPool;
        this.workflowDao = workflowDao;
        this.garbageCollector = gc;
        this.operationDao = operationDao;
        this.graphDao = graphDao;
        this.storage = storage;
    }

    @Override
    public void createWorkflow(CreateWorkflowRequest request, StreamObserver<CreateWorkflowResponse> responseObserver) {
        workflowService.createWorkflow(request, responseObserver);
    }

    @Override
    public void attachWorkflow(AttachWorkflowRequest request, StreamObserver<AttachWorkflowResponse> responseObserver) {
        workflowService.attachWorkflow(request, responseObserver);
    }

    @Override
    public void finishWorkflow(FinishWorkflowRequest request, StreamObserver<FinishWorkflowResponse> responseObserver) {
        workflowService.finishWorkflow(request, responseObserver);
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

        var op = Operation.create(userId, "Execute graph in execution: executionId='%s'".formatted(executionId),
            idempotencyKey, null);

        String parentGraphId = request.getGraph().getParentGraphId();
        String zone = request.getGraph().getZone();
        List<LWF.Operation> operations = request.getGraph().getOperationsList();
        List<LWF.DataDescription> descriptions = request.getGraph().getDataDescriptionsList();

        var state = new GraphExecutionState(executionId, op.id(), parentGraphId, userId, zone, descriptions,
            operations);

        try (var transaction = TransactionHandle.create(storage)) {
            withRetries(LOG, () -> operationDao.create(op, transaction));
            withRetries(LOG, () -> graphDao.save(state, transaction));

            transaction.commit();
        } catch (Exception ex) {
            if (idempotencyKey != null && handleIdempotencyKeyConflict(idempotencyKey, ex, operationDao,
                responseObserver, ExecuteGraphResponse.class, Duration.ofMillis(100), Duration.ofSeconds(5), LOG))
            {
                return;
            }

            LOG.error("Cannot create execute graph operation for: { userId: {}, executionId: {}, error: {}}",
                userId, executionId, ex.getMessage(), ex);
            var status = Status.INTERNAL.withDescription(ex.getMessage());
            updateExecutionStatus(userId, executionId, status);
            responseObserver.onError(status.asException());
            return;
        }

        Future<Operation> completedOpFuture = workersPool.submit(() -> graphExecutionService.executeGraph(state));
        try {
            var completedOp = completedOpFuture.get(30, TimeUnit.SECONDS);
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
            updateExecutionStatus(userId, executionId, status);
            responseObserver.onError(status.asRuntimeException());
        }
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

    private void updateExecutionStatus(String userId, String executionId, Status status) {
        try {
            withRetries(defaultRetryPolicy(), LOG, () -> {
                try (var transaction = TransactionHandle.create(storage)) {
                    var workflowName = workflowDao.getWorkflowName(executionId);

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
