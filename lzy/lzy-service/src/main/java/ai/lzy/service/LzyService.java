package ai.lzy.service;

import ai.lzy.iam.grpc.context.AuthenticationContext;
import ai.lzy.longrunning.IdempotencyUtils;
import ai.lzy.longrunning.Operation;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.Storage;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.NotFoundException;
import ai.lzy.service.config.LzyServiceConfig;
import ai.lzy.service.data.dao.ExecutionDao;
import ai.lzy.service.data.dao.GraphDao;
import ai.lzy.service.data.dao.WorkflowDao;
import ai.lzy.service.data.storage.LzyServiceStorage;
import ai.lzy.service.gc.GarbageCollector;
import ai.lzy.service.graph.GraphExecutionService;
import ai.lzy.service.graph.GraphExecutionState;
import ai.lzy.service.graph.debug.InjectedFailures;
import ai.lzy.service.util.StorageUtils;
import ai.lzy.service.workflow.WorkflowService;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.util.grpc.ProtoPrinter;
import ai.lzy.v1.common.LMST;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.storage.LSS;
import ai.lzy.v1.storage.LzyStorageServiceGrpc;
import ai.lzy.v1.workflow.LWF;
import ai.lzy.v1.workflow.LzyWorkflowServiceGrpc;
import com.google.common.annotations.VisibleForTesting;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;

import java.net.URI;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;

import static ai.lzy.longrunning.IdempotencyUtils.handleIdempotencyKeyConflict;
import static ai.lzy.longrunning.IdempotencyUtils.loadExistingOp;
import static ai.lzy.longrunning.IdempotencyUtils.loadExistingOpResult;
import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.v1.workflow.LWFS.*;

@Singleton
public class LzyService extends LzyWorkflowServiceGrpc.LzyWorkflowServiceImplBase {
    private static final Logger LOG = LogManager.getLogger(LzyService.class);

    public static final String APP = "LzyService";

    private final CleanExecutionCompanion cleanExecutionCompanion;
    private final String instanceId;

    private final WorkflowService workflowService;
    private final GraphExecutionService graphExecutionService;

    private final ExecutorService workersPool;

    private final LzyStorageServiceGrpc.LzyStorageServiceBlockingStub storageServiceClient;

    private final Storage storage;
    private final OperationDao operationDao;
    private final WorkflowDao workflowDao;
    private final ExecutionDao executionDao;
    private final GraphDao graphDao;

    public LzyService(WorkflowService workflowService, GraphExecutionService graphExecutionService,
                      GraphDao graphDao, ExecutionDao executionDao, WorkflowDao workflowDao,
                      LzyServiceStorage storage, @Named("LzyServiceOperationDao") OperationDao operationDao,
                      CleanExecutionCompanion cleanExecutionCompanion, GarbageCollector gc, LzyServiceConfig config,
                      @Named("LzyServiceIamToken") RenewableJwt internalUserCredentials,
                      @Named("StorageServiceChannel") ManagedChannel storageChannel,
                      @Named("LzyServiceServerExecutor") ExecutorService workersPool)
    {
        this.cleanExecutionCompanion = cleanExecutionCompanion;
        this.instanceId = config.getInstanceId();
        this.workflowService = workflowService;
        this.graphExecutionService = graphExecutionService;
        this.workersPool = workersPool;
        this.operationDao = operationDao;
        this.workflowDao = workflowDao;
        this.executionDao = executionDao;
        this.graphDao = graphDao;
        this.storage = storage;

        this.storageServiceClient = newBlockingClient(
            LzyStorageServiceGrpc.newBlockingStub(storageChannel), APP, () -> internalUserCredentials.get().token());

        // gc.start();

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
    public void startWorkflow(StartWorkflowRequest request, StreamObserver<StartWorkflowResponse> responseObserver) {
        workflowService.startWorkflow(request, responseObserver);
    }

    @Override
    public void finishWorkflow(FinishWorkflowRequest request, StreamObserver<LongRunning.Operation> response) {
        Operation.IdempotencyKey idempotencyKey = IdempotencyUtils.getIdempotencyKey(request);
        if (idempotencyKey != null && loadExistingOp(operationDao, idempotencyKey, response, LOG)) {
            return;
        }

        var userId = AuthenticationContext.currentSubject().id();
        var workflowName = request.getWorkflowName();
        var executionId = request.getExecutionId();
        var reason = request.getReason();

        if (Strings.isBlank(executionId) || Strings.isBlank(workflowName)) {
            LOG.error("Cannot finish workflow. Blank 'executionId' or 'workflowName': {}",
                ProtoPrinter.printer().printToString(request));
            response.onError(Status.INVALID_ARGUMENT.withDescription("Blank 'executionId' or 'workflowName'")
                .asRuntimeException());
            return;
        }

        LOG.info("Attempt to finish workflow: { userId: {}, workflowName: {}, executionId: {}, reason: {} }", userId,
            workflowName, executionId, reason);

        var op = Operation.create(userId, "Finish workflow: workflowName='%s', executionId='%s'"
            .formatted(workflowName, executionId), null, idempotencyKey, null);
        var finishStatus = Status.OK.withDescription(reason);

        try (var tx = TransactionHandle.create(storage)) {
            executionDao.updateFinishData(userId, workflowName, executionId, finishStatus, tx);
            operationDao.create(op, tx);
            executionDao.setCompletingExecutionStatus(executionId, tx);

            tx.commit();
        } catch (NotFoundException e) {
            LOG.error("Cannot finish workflow, not found: { workflowName: {}, executionId: {}, error: {} }",
                workflowName, executionId, e.getMessage());
            response.onError(Status.NOT_FOUND.withDescription(e.getMessage()).asRuntimeException());
            return;
        } catch (IllegalStateException e) {
            LOG.error("Cannot finish workflow, invalid state: { workflowName: {}, executionId: {}, error: {} }",
                workflowName, executionId, e.getMessage());
            response.onError(Status.FAILED_PRECONDITION.withDescription(e.getMessage()).asRuntimeException());
            return;
        } catch (Exception e) {
            if (idempotencyKey != null &&
                handleIdempotencyKeyConflict(idempotencyKey, e, operationDao, response, LOG))
            {
                return;
            }

            LOG.error("Unexpected error while finish workflow: { workflowName: {}, executionId: {}, error: {} }",
                workflowName, executionId, e.getMessage());
            response.onError(Status.INTERNAL.withDescription("Cannot finish execution " +
                "'%s': %s".formatted(executionId, e.getMessage())).asRuntimeException());
            return;
        }

        workersPool.submit(() -> workflowService.completeExecution(executionId, op));

        response.onNext(op.toProto());
        response.onCompleted();
    }

    @Override
    public void abortWorkflow(AbortWorkflowRequest request, StreamObserver<AbortWorkflowResponse> response) {
        var userId = AuthenticationContext.currentSubject().id();
        var workflowName = request.getWorkflowName();
        var executionId = request.getExecutionId();
        var reason = request.getReason();

        if (Strings.isBlank(workflowName) || Strings.isBlank(executionId)) {
            LOG.error("Empty 'workflowName' or 'executionId': {}", ProtoPrinter.printer().printToString(request));
            response.onError(Status.INVALID_ARGUMENT.withDescription("Empty 'workflowName' or 'executionId'")
                .asRuntimeException());
            return;
        }

        LOG.info("Attempt to abort workflow with active execution: { workflowName: {}, executionId: {} }",
            workflowName, executionId);

        var abortStatus = Status.CANCELLED.withDescription(reason);
        try {
            cleanExecutionCompanion.markExecutionAsBroken(userId, workflowName, executionId, abortStatus);
            cleanExecutionCompanion.stopGraphs(executionId);
        } catch (IllegalStateException ise) {
            LOG.error("Execution from argument is not an active in workflow: " +
                "{ userId: {}, workflowName: {}, executionId: {} }", userId, workflowName, executionId);
            response.onError(Status.FAILED_PRECONDITION.withDescription("Cannot abort user workflow " +
                "'%s'. Execution '%s' is not an active".formatted(workflowName, executionId)).asRuntimeException());
            return;
        } catch (NotFoundException nfe) {
            LOG.error("Workflow with active execution not found: { userId: {}, workflowName: {}, executionId: {} }",
                userId, workflowName, executionId);
            response.onError(Status.NOT_FOUND.withDescription("Cannot found user workflow " +
                "'%s' with active execution '%s'".formatted(workflowName, executionId)).asRuntimeException());
            return;
        } catch (Exception e) {
            LOG.error("Cannot abort workflow: { userId: {}, workflowName: {}, executionId: {} }",
                userId, workflowName, executionId, e);
            response.onError(Status.INTERNAL.withDescription("Cannot abort workflow").asRuntimeException());
            return;
        }

        response.onNext(AbortWorkflowResponse.getDefaultInstance());
        response.onCompleted();
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
        String workflowName = request.getWorkflowName();
        String executionId = request.getExecutionId();

        if (checkExecution(userId, executionId, responseObserver)) {
            return;
        }

        var op = Operation.create(userId, "Execute graph in execution: executionId='%s'".formatted(executionId),
            null, idempotencyKey, null);

        String parentGraphId = request.getGraph().getParentGraphId();
        String zone = request.getGraph().getZone();
        List<LWF.Operation> operations = request.getGraph().getOperationsList();
        List<LWF.DataDescription> descriptions = request.getGraph().getDataDescriptionsList();

        var state = new GraphExecutionState(workflowName, executionId, op.id(), parentGraphId, userId, zone,
            descriptions, operations);

        try (var tx = TransactionHandle.create(storage)) {
            operationDao.create(op, tx);
            graphDao.put(state, instanceId, tx);

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

            if (cleanExecutionCompanion.tryToMarkExecutionAsBroken(userId, workflowName, executionId,
                status))
            {
                cleanExecutionCompanion.cleanExecution(executionId);
            }

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

                if (cleanExecutionCompanion.tryToMarkExecutionAsBroken(userId, workflowName, executionId, status)) {
                    cleanExecutionCompanion.cleanExecution(executionId);
                }

                responseObserver.onError(status.asRuntimeException());
            }
        });
    }

    @Override
    public void graphStatus(GraphStatusRequest request, StreamObserver<GraphStatusResponse> responseObserver) {
        String userId = AuthenticationContext.currentSubject().id();
        String executionId = request.getExecutionId();

        if (checkExecution(userId, executionId, responseObserver)) {
            return;
        }

        graphExecutionService.graphStatus(request, responseObserver);
    }

    @Override
    public void stopGraph(StopGraphRequest request, StreamObserver<StopGraphResponse> responseObserver) {
        String userId = AuthenticationContext.currentSubject().id();
        String executionId = request.getExecutionId();

        if (checkExecution(userId, executionId, responseObserver)) {
            return;
        }

        graphExecutionService.stopGraph(request, responseObserver);
    }

    @Override
    public void readStdSlots(ReadStdSlotsRequest request, StreamObserver<ReadStdSlotsResponse> responseObserver) {
        String userId = AuthenticationContext.currentSubject().id();
        String executionId = request.getExecutionId();

        if (checkExecution(userId, executionId, responseObserver)) {
            return;
        }

        workflowService.readStdSlots(request, responseObserver);
    }

    @Override
    public void getAvailablePools(GetAvailablePoolsRequest request,
                                  StreamObserver<GetAvailablePoolsResponse> responseObserver)
    {
        String userId = AuthenticationContext.currentSubject().id();
        String executionId = request.getExecutionId();

        if (checkExecution(userId, executionId, responseObserver)) {
            return;
        }

        workflowService.getAvailablePools(request, responseObserver);
    }

    @Override
    public void getStorageCredentials(GetStorageCredentialsRequest request,
                                      StreamObserver<GetStorageCredentialsResponse> responseObserver)
    {
        final String userId = AuthenticationContext.currentSubject().id();
        final String bucketName = StorageUtils.createInternalBucketName(userId);

        LOG.info("Get storage credentials for bucket {}", bucketName);

        // No checkExecution, can be called out of workflow

        var credsRequest = LSS.GetS3BucketCredentialsRequest.newBuilder()
            .setUserId(userId)
            .setBucket(bucketName)
            .build();

        final LMST.StorageConfig storageConfig;
        try {
            var credsResponse = storageServiceClient.getS3BucketCredentials(credsRequest);

            storageConfig = switch (credsResponse.getCredentialsCase()) {
                case AMAZON -> LMST.StorageConfig.newBuilder().setS3(credsResponse.getAmazon())
                    .setUri(URI.create("s3://" + bucketName).toString())
                    .build();
                case AZURE -> LMST.StorageConfig.newBuilder().setAzure(credsResponse.getAzure())
                    .setUri(URI.create("azure://" + bucketName).toString())
                    .build();
                default -> null;
            };

            if (storageConfig == null) {
                LOG.error("Unsupported bucket storage type {}", credsResponse.getCredentialsCase());
                responseObserver.onError(Status.INTERNAL.withDescription("Failed to resolve storage credentials")
                    .asException());
                return;
            }
        } catch (StatusRuntimeException e) {
            if (Status.NOT_FOUND.getCode() == e.getStatus().getCode()) {
                responseObserver.onError(Status.NOT_FOUND.asException());
                return;
            }
            LOG.error("Failed to get storage credentials: {}", e.getStatus().getDescription());
            responseObserver.onError(Status.INTERNAL.withDescription("Failed to resolve storage credentials")
                .asException());
            return;
        }

        responseObserver.onNext(GetStorageCredentialsResponse.newBuilder().setStorage(storageConfig).build());
        LOG.info("Get storage credentials for bucket {} done", bucketName);
        responseObserver.onCompleted();
    }

    private <T> boolean checkExecution(String userId, String executionId, StreamObserver<T> responseObserver) {
        try {
            WorkflowDao.WorkflowInfo wfNameAndUserId = withRetries(LOG, () -> workflowDao.findWorkflowBy(executionId));
            if (wfNameAndUserId == null || !Objects.equals(userId, wfNameAndUserId.userId())) {
                LOG.error("Cannot find active execution of user: { executionId: {}, userId: {} }", executionId, userId);
                responseObserver.onError(Status.NOT_FOUND.withDescription("Cannot find active execution " +
                    "'%s' of user '%s'".formatted(executionId, userId)).asRuntimeException());
                return true;
            }
        } catch (Exception e) {
            LOG.error("Cannot check that active execution of user exists: { executionId: {}, userId: " +
                "{}, error: {} } ", executionId, userId, e.getMessage());
            responseObserver.onError(Status.INTERNAL.withDescription("Cannot execute graph").asRuntimeException());
            return true;
        }

        return false;
    }
}
