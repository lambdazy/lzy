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
import ai.lzy.service.debug.InjectedFailures;
import ai.lzy.service.graph.GraphExecutionService;
import ai.lzy.service.graph.GraphExecutionState;
import ai.lzy.service.util.StorageUtils;
import ai.lzy.service.workflow.WorkflowService;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.util.grpc.ProtoPrinter;
import ai.lzy.v1.common.LMST;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import ai.lzy.v1.storage.LSS;
import ai.lzy.v1.storage.LzyStorageServiceGrpc;
import ai.lzy.v1.workflow.LzyWorkflowServiceGrpc;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.micronaut.core.util.StringUtils;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;

import java.net.URI;
import java.sql.SQLException;
import java.time.Duration;
import java.util.HashSet;
import java.util.Objects;
import java.util.concurrent.ExecutorService;

import static ai.lzy.longrunning.IdempotencyUtils.*;
import static ai.lzy.longrunning.OperationUtils.awaitOperationDone;
import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.withIdempotencyKey;
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
    private final LongRunningServiceGrpc.LongRunningServiceBlockingStub storageOpService;
    private final Duration bucketCreationTimeout;

    private final Storage storage;
    private final LzyServiceMetrics metrics;
    private final OperationDao operationDao;
    private final WorkflowDao workflowDao;
    private final ExecutionDao executionDao;
    private final GraphDao graphDao;

    public LzyService(WorkflowService workflowService, GraphExecutionService graphExecutionService,
                      GraphDao graphDao, ExecutionDao executionDao, WorkflowDao workflowDao,
                      LzyServiceStorage storage, @Named("LzyServiceOperationDao") OperationDao operationDao,
                      CleanExecutionCompanion cleanExecutionCompanion, LzyServiceConfig config,
                      @Named("LzyServiceIamToken") RenewableJwt internalUserCredentials,
                      @Named("StorageServiceChannel") ManagedChannel storageChannel
        /*, GarbageCollector gc */, @Named("LzyServiceServerExecutor") ExecutorService workersPool,
                      LzyServiceMetrics metrics)
    {
        this.cleanExecutionCompanion = cleanExecutionCompanion;
        this.instanceId = config.getInstanceId();
        this.bucketCreationTimeout = config.getStorage().getBucketCreationTimeout();
        this.workflowService = workflowService;
        this.graphExecutionService = graphExecutionService;
        this.workersPool = workersPool;
        this.operationDao = operationDao;
        this.workflowDao = workflowDao;
        this.executionDao = executionDao;
        this.graphDao = graphDao;
        this.storage = storage;
        this.metrics = metrics;

        this.storageServiceClient = newBlockingClient(
            LzyStorageServiceGrpc.newBlockingStub(storageChannel), APP, () -> internalUserCredentials.get().token());
        this.storageOpService = newBlockingClient(
            LongRunningServiceGrpc.newBlockingStub(storageChannel), APP, () -> internalUserCredentials.get().token());

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

                var activeExecutions = new HashSet<String>();
                execGraphStates.forEach(state -> {
                    if (activeExecutions.add(state.getExecutionId())) {
                        metrics.activeExecutions.labels(state.getUserId()).inc();
                    }
                    workersPool.submit(() -> graphExecutionService.executeGraph(state));
                });
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

        try {
            withRetries(LOG, () -> {
                try (var tx = TransactionHandle.create(storage)) {
                    workflowDao.setActiveExecutionToNull(userId, workflowName, executionId, tx);
                    executionDao.updateFinishData(userId, executionId, finishStatus, tx);
                    operationDao.create(op, tx);
                    executionDao.setCompletingExecutionStatus(executionId, tx);

                    tx.commit();
                }
            });
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

        workersPool.submit(() -> workflowService.completeExecution(userId, executionId, op));

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
            cleanExecutionCompanion.finishWorkflow(userId, workflowName, executionId, abortStatus);
            cleanExecutionCompanion.cleanExecution(executionId);
        } catch (NotFoundException nfe) {
            LOG.error("Workflow with active execution not found: { userId: {}, workflowName: {}, executionId: {} }",
                userId, workflowName, executionId);
            response.onError(Status.NOT_FOUND.withDescription("Cannot found user workflow " +
                "'%s' with active execution '%s'".formatted(workflowName, executionId)).asRuntimeException());
            return;
        } catch (Exception e) {
            LOG.error("Cannot abort workflow: { userId: {}, workflowName: {}, executionId: {}, error: {} }",
                userId, workflowName, executionId, e.getMessage(), e);
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

        var state = new GraphExecutionState(userId, workflowName, executionId, op.id(), request.getGraph());

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

            if (cleanExecutionCompanion.tryToFinishWorkflow(userId, workflowName, executionId,
                status))
            {
                cleanExecutionCompanion.cleanExecution(executionId);
            }

            responseObserver.onError(status.asException());
            return;
        }

        InjectedFailures.fail0();

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

                if (cleanExecutionCompanion.tryToFinishWorkflow(userId, workflowName, executionId, status)) {
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
    public void getOrCreateDefaultStorage(GetOrCreateDefaultStorageRequest request,
                                          StreamObserver<GetOrCreateDefaultStorageResponse> responseObserver)
    {
        final String userId = AuthenticationContext.currentSubject().id();
        final String bucketName = StorageUtils.createInternalBucketName(userId);

        LOG.info("Get storage credentials for bucket {}", bucketName);
        final LMST.StorageConfig storageConfig;
        try {
            LOG.info("Creating new temporary storage bucket if it does not exist: { bucketName: {}, userId: {} }",
                bucketName, userId);
            LongRunning.Operation createOp =
                withIdempotencyKey(storageServiceClient, userId + "_" + bucketName)
                    .createStorage(LSS.CreateStorageRequest.newBuilder()
                        .setUserId(userId)
                        .setBucket(bucketName)
                        .build());

            createOp = awaitOperationDone(storageOpService, createOp.getId(), bucketCreationTimeout);
            if (!createOp.getDone()) {
                try {
                    // do not wait until op is cancelled here
                    //noinspection ResultOfMethodCallIgnored
                    storageOpService.cancel(
                        LongRunning.CancelOperationRequest.newBuilder().setOperationId(createOp.getId()).build());
                } finally {
                    responseObserver.onError(Status.DEADLINE_EXCEEDED.withDescription(
                        "Cannot wait create bucket operation response: { opId: {} }" +
                            createOp.getId()).asException());
                }
            }

            if (createOp.hasError()) {
                var status = createOp.getError();
                responseObserver.onError(Status.fromCodeValue(status.getCode())
                    .withDescription("Cannot process create S3 bucket operation: " +
                        "{ operationId: %s }, error: %s".formatted(createOp.getId(), status.getMessage()))
                    .asException());
                return;
            }

            LSS.CreateStorageResponse response = createOp.getResponse().unpack(LSS.CreateStorageResponse.class);
            storageConfig = switch (response.getCredentialsCase()) {
                case S3 -> LMST.StorageConfig.newBuilder().setS3(response.getS3())
                    .setUri(URI.create("s3://" + bucketName).toString())
                    .build();
                case AZURE -> LMST.StorageConfig.newBuilder().setAzure(response.getAzure())
                    .setUri(URI.create("azure://" + bucketName).toString())
                    .build();
                default -> null;
            };

            if (storageConfig == null) {
                deleteTempUserBucket(bucketName);
                LOG.error("Unsupported bucket storage type {}", response.getCredentialsCase());
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
            responseObserver.onError(e.getStatus().asException());
            return;
        } catch (InvalidProtocolBufferException e) {
            LOG.error("Cannot deserialize create S3 bucket response from operation: " + e.getMessage());
            responseObserver.onError(
                Status.INTERNAL.withDescription("Cannot create temp bucket: " + e.getMessage()).asException());
            return;
        }

        responseObserver.onNext(GetOrCreateDefaultStorageResponse.newBuilder().setStorage(storageConfig).build());
        LOG.info("Get storage credentials for bucket {} done", bucketName);
        responseObserver.onCompleted();
    }

    private <T> boolean checkExecution(String userId, String executionId, StreamObserver<T> responseObserver) {
        try {
            WorkflowDao.WorkflowInfo wfNameAndUserId = withRetries(LOG, () -> workflowDao.findWorkflowBy(executionId));
            if (wfNameAndUserId == null || !Objects.equals(userId, wfNameAndUserId.userId())) {
                LOG.error("Cannot find active execution of user: { executionId: {}, userId: {} }", executionId, userId);
                responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Cannot find active execution " +
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

    private void deleteTempUserBucket(String bucket) {
        if (StringUtils.isEmpty(bucket)) {
            return;
        }

        LOG.info("Deleting temp storage bucket '{}'", bucket);

        try {
            @SuppressWarnings("unused")
            var resp = storageServiceClient.deleteStorage(
                LSS.DeleteStorageRequest.newBuilder()
                    .setBucket(bucket)
                    .build());
        } catch (StatusRuntimeException e) {
            LOG.error("Can't delete temp bucket '{}': ({}) {}", bucket, e.getStatus(), e.getMessage(), e);
        }
    }
}
