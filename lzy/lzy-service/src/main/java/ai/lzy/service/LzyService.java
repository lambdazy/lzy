package ai.lzy.service;

import ai.lzy.longrunning.IdempotencyUtils;
import ai.lzy.longrunning.Operation;
import ai.lzy.longrunning.OperationsExecutor;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.Storage;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.InvalidStateException;
import ai.lzy.model.db.exceptions.NotFoundException;
import ai.lzy.service.config.LzyServiceConfig;
import ai.lzy.service.data.ExecuteGraphOperationState;
import ai.lzy.service.data.StartExecutionOperationState;
import ai.lzy.service.data.StopExecutionOperationState;
import ai.lzy.service.data.dao.ExecutionDao;
import ai.lzy.service.data.dao.ExecutionOperationsDao;
import ai.lzy.service.data.dao.GraphDao;
import ai.lzy.service.data.dao.WorkflowDao;
import ai.lzy.service.data.storage.LzyServiceStorage;
import ai.lzy.service.graph.GraphExecutionService;
import ai.lzy.service.util.StorageUtils;
import ai.lzy.service.workflow.WorkflowService;
import ai.lzy.service.workflow.finish.AbortExecutionAction;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.v1.common.LMST;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc.LongRunningServiceBlockingStub;
import ai.lzy.v1.storage.LSS;
import ai.lzy.v1.storage.LzyStorageServiceGrpc;
import ai.lzy.v1.storage.LzyStorageServiceGrpc.LzyStorageServiceBlockingStub;
import ai.lzy.v1.workflow.LWF;
import ai.lzy.v1.workflow.LzyWorkflowServiceGrpc;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageOrBuilder;
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
import java.time.Duration;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import static ai.lzy.iam.grpc.context.AuthenticationContext.currentSubject;
import static ai.lzy.longrunning.IdempotencyUtils.*;
import static ai.lzy.longrunning.OperationUtils.awaitOperationDone;
import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.withIdempotencyKey;
import static ai.lzy.util.grpc.ProtoConverter.toProto;
import static ai.lzy.util.grpc.ProtoPrinter.safePrinter;
import static ai.lzy.v1.workflow.LWFS.*;

@Singleton
public class LzyService extends LzyWorkflowServiceGrpc.LzyWorkflowServiceImplBase {
    private static final Logger LOG = LogManager.getLogger(LzyService.class);
    public static final String APP = "LzyService";

    private final String instanceId;

    private final Storage storage;
    private final OperationDao operationDao;
    private final WorkflowDao workflowDao;
    private final ExecutionDao executionDao;
    private final ExecutionOperationsDao executionOperationsDao;
    private final GraphDao graphDao;

    private final WorkflowService workflowService;
    private final GraphExecutionService graphExecutionService;
    private final LzyStorageServiceBlockingStub storagesClient;
    private final LongRunningServiceBlockingStub storageOpsClient;

    private final LzyServiceConfig config;
    private final ActionsManager actionsManager;
    private final Duration bucketCreationTimeout;
    private final LzyServiceMetrics metrics;

    public LzyService(WorkflowService workflowService, GraphExecutionService graphExecutionService,
                      GraphDao graphDao, ExecutionDao executionDao, WorkflowDao workflowDao,
                      LzyServiceStorage storage, @Named("LzyServiceOperationDao") OperationDao operationDao,
                      ExecutionOperationsDao executionOperationsDao,
                      LzyServiceConfig config, ActionsManager actionsManager,
                      @Named("LzyServiceIamToken") RenewableJwt internalUserCredentials,
                      @Named("StorageServiceChannel") ManagedChannel storageChannel,
                      @Named("LzyServiceOperationsExecutor") OperationsExecutor operationsExecutor,
                      LzyServiceMetrics metrics)
    {
        this.instanceId = config.getInstanceId();
        this.bucketCreationTimeout = config.getStorage().getBucketCreationTimeout();
        this.workflowService = workflowService;
        this.graphExecutionService = graphExecutionService;
        this.operationDao = operationDao;
        this.workflowDao = workflowDao;
        this.executionDao = executionDao;
        this.graphDao = graphDao;
        this.storage = storage;
        this.config = config;
        this.actionsManager = actionsManager;
        this.metrics = metrics;
        this.executionOperationsDao = executionOperationsDao;

        this.storagesClient = newBlockingClient(
            LzyStorageServiceGrpc.newBlockingStub(storageChannel), APP, () -> internalUserCredentials.get().token());
        this.storageOpsClient = newBlockingClient(
            LongRunningServiceGrpc.newBlockingStub(storageChannel), APP, () -> internalUserCredentials.get().token());

        // restartNotCompletedOps();
    }

    /*
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
    */

    private boolean validate(StartWorkflowRequest request, StreamObserver<? extends MessageOrBuilder> response) {
        LOG.debug("Validate StartWorkflowRequest: {}", safePrinter().printToString(request));
        if (Strings.isBlank(request.getWorkflowName())) {
            LOG.error("Cannot start workflow execution. Blank 'workflowName': {}",
                safePrinter().printToString(request));
            response.onError(Status.INVALID_ARGUMENT.withDescription("Blank 'workflowName'").asRuntimeException());
            return true;
        }

        return false;
    }

    private boolean validate(String userId, FinishWorkflowRequest request,
                             StreamObserver<? extends MessageOrBuilder> response)
    {
        LOG.debug("Validate FinishWorkflowRequest: {}", safePrinter().printToString(request));
        if (Strings.isBlank(request.getExecutionId()) || Strings.isBlank(request.getWorkflowName())) {
            LOG.error("Cannot finish workflow. Blank 'executionId' or 'workflowName': {}",
                safePrinter().printToString(request));
            response.onError(Status.INVALID_ARGUMENT.withDescription("Blank 'executionId' or 'workflowName'")
                .asRuntimeException());
            return true;
        }

        return checkPermissionOnExecution(userId, request.getExecutionId(), response);
    }

    private boolean validate(String userId, AbortWorkflowRequest request,
                             StreamObserver<? extends MessageOrBuilder> response)
    {
        LOG.debug("Validate AbortWorkflowRequest: {}", safePrinter().printToString(request));
        if (Strings.isBlank(request.getExecutionId()) || Strings.isBlank(request.getWorkflowName())) {
            LOG.error("Cannot abort workflow. Blank 'executionId' or 'workflowName': {}",
                safePrinter().printToString(request));
            response.onError(Status.INVALID_ARGUMENT.withDescription("Blank 'executionId' or 'workflowName'")
                .asRuntimeException());
            return true;
        }

        return checkPermissionOnExecution(userId, request.getExecutionId(), response);
    }

    private boolean validate(String userId, ExecuteGraphRequest request,
                             StreamObserver<? extends MessageOrBuilder> response)
    {
        LOG.debug("Validate ExecuteGraphRequest: {}", safePrinter().printToString(request));
        if (Strings.isBlank(request.getExecutionId()) || Strings.isBlank(request.getWorkflowName())) {
            LOG.error("Cannot execute graph in workflow execution. Blank 'executionId' or 'workflowName': {}",
                safePrinter().printToString(request));
            response.onError(Status.INVALID_ARGUMENT.withDescription("Blank 'executionId' or 'workflowName'")
                .asRuntimeException());
            return true;
        }

        return checkPermissionOnExecution(userId, request.getExecutionId(), response);
    }

    private boolean validate(String userId, GraphStatusRequest request,
                             StreamObserver<? extends MessageOrBuilder> response)
    {
        LOG.debug("Validate GraphStatusRequest: {}", safePrinter().printToString(request));
        if (Strings.isBlank(request.getExecutionId()) || Strings.isBlank(request.getGraphId())) {
            LOG.error("Cannot obtain graph status. Blank 'executionId' or 'graphId': {}",
                safePrinter().printToString(request));
            response.onError(Status.INVALID_ARGUMENT.withDescription("Blank 'executionId' or 'graphId'")
                .asRuntimeException());
            return true;
        }

        return checkPermissionOnExecution(userId, request.getExecutionId(), response);
    }

    private boolean validate(String userId, StopGraphRequest request,
                             StreamObserver<? extends MessageOrBuilder> response)
    {
        LOG.debug("Validate StopGraphRequest: {}", safePrinter().printToString(request));
        if (Strings.isBlank(request.getExecutionId()) || Strings.isBlank(request.getGraphId())) {
            LOG.error("Cannot stop graph. Blank 'executionId' or 'graphId': {}",
                safePrinter().printToString(request));
            response.onError(Status.INVALID_ARGUMENT.withDescription("Blank 'executionId' or 'graphId'")
                .asRuntimeException());
            return true;
        }

        return checkPermissionOnExecution(userId, request.getExecutionId(), response);
    }

    @Override
    public void startWorkflow(StartWorkflowRequest request, StreamObserver<LongRunning.Operation> responseObserver) {
        var userId = currentSubject().id();
        var wfName = request.getWorkflowName();
        var newExecId = wfName + "_" + UUID.randomUUID();

        LOG.info("Request to start workflow execution: { userId: {}, wfName: {}, newActiveExecId: {} }", userId,
            wfName, newExecId);

        if (validate(request, responseObserver)) {
            return;
        }

        Operation.IdempotencyKey idempotencyKey = IdempotencyUtils.getIdempotencyKey(request);
        if (idempotencyKey != null && loadExistingOp(operationDao, idempotencyKey, responseObserver, LOG)) {
            return;
        }

        var startOpTimeout = config.getWaitAllocationTimeout().plus(Duration.ofSeconds(15));

        Operation startOp = Operation.create(userId, String.format("Start workflow execution: wfName='%s', " +
            "newActiveExecId=%s", wfName, newExecId), startOpTimeout, idempotencyKey, null);
        Operation stopOp = null;
        String oldExecId;

        try (var tx = TransactionHandle.create(storage)) {
            oldExecId = workflowDao.upsert(userId, wfName, newExecId, tx);

            if (oldExecId != null) {
                stopOp = Operation.create(userId, "Stop execution: execId='%s'".formatted(oldExecId),
                    AbortExecutionAction.timeout, idempotencyKey, null);
                operationDao.create(stopOp, tx);
                executionOperationsDao.create(stopOp.id(), instanceId, oldExecId,
                    new StopExecutionOperationState(), tx);
            }

            operationDao.create(startOp, tx);
            executionOperationsDao.create(startOp.id(), instanceId, newExecId, new StartExecutionOperationState(), tx);
            tx.commit();
        } catch (Exception e) {
            if (idempotencyKey != null &&
                handleIdempotencyKeyConflict(idempotencyKey, e, operationDao, responseObserver, LOG))
            {
                return;
            }

            LOG.error("Cannot start workflow: { userId: {}, wfName: {}, error: {} }", userId, wfName,
                e.getMessage(), e);

            responseObserver.onError(Status.INTERNAL.withDescription("Cannot start workflow").asRuntimeException());
            return;
        }

        var idk = idempotencyKey != null ? idempotencyKey.token() : null;
        try {
            LOG.debug("Schedule action to start execution: { userId: {}, wfName: {} }", userId, wfName);
            actionsManager.startExecutionAction(startOp.id(), startOp.description(), idk, userId, wfName, newExecId);
        } catch (Exception e) {
            LOG.error("Cannot schedule action to start new workflow execution: { userId: {}, wfName: {}, error: {} } ",
                userId, wfName, e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withDescription("Cannot start workflow").asRuntimeException());
            return;
        }
        try {
            if (stopOp != null) {
                LOG.debug("Schedule action to abort previous execution: { userId: {}, wfName: {} }", userId, wfName);
                actionsManager.abortExecutionAction(stopOp.id(), stopOp.description(), idk, oldExecId,
                    Status.CANCELLED.withDescription("by new started execution"));
            }
        } catch (Exception e) {
            LOG.error("Cannot schedule action to finish previous active execution: { userId: {}, wfName: {}," +
                " error: {} }", userId, wfName, e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withDescription("Cannot start workflow").asRuntimeException());
            return;
        }

        responseObserver.onNext(startOp.toProto());
        responseObserver.onCompleted();
    }

    @Override
    public void finishWorkflow(FinishWorkflowRequest request, StreamObserver<FinishWorkflowResponse> responseObserver) {
        var userId = currentSubject().id();
        var wfName = request.getWorkflowName();
        var execId = request.getExecutionId();
        var reason = request.getReason();

        LOG.info("Request to finish workflow with active execution: { userId: {}, wfName: {}, execId: {}, " +
            "reason: {} }", userId, wfName, execId, reason);

        if (validate(userId, request, responseObserver)) {
            return;
        }

        var checkOpResultDelay = Duration.ofMillis(300);
        var opTimeout = Duration.ofSeconds(15);
        Operation.IdempotencyKey idempotencyKey = IdempotencyUtils.getIdempotencyKey(request);
        if (idempotencyKey != null && loadExistingOpResult(operationDao, idempotencyKey, responseObserver,
            FinishWorkflowResponse.class, checkOpResultDelay, opTimeout, LOG))
        {
            return;
        }

        boolean[] success = {false};
        var op = Operation.create(userId, "Finish workflow execution: wfName='%s', activeExecId='%s'".formatted(
            wfName, execId), opTimeout, idempotencyKey, null);

        try {
            withRetries(LOG, () -> {
                try (var tx = TransactionHandle.create(storage)) {
                    success[0] = workflowDao.deactivate(userId, wfName, execId, tx);
                    if (success[0]) {
                        var opsToCancel = executionOperationsDao.get(execId, tx).stream()
                            .filter(opInfo -> opInfo.type() != ExecutionOperationsDao.OpType.STOP)
                            .map(ExecutionOperationsDao.OpInfo::opId)
                            .toList();
                        if (!opsToCancel.isEmpty()) {
                            operationDao.cancel(opsToCancel, toProto(Status.CANCELLED.withDescription("Execution " +
                                "was finished")), tx);
                        }

                        operationDao.create(op, tx);
                        executionOperationsDao.create(op.id(), instanceId, execId, new StopExecutionOperationState(),
                            tx);
                    }
                    tx.commit();
                }
            });
        } catch (Exception e) {
            if (idempotencyKey != null && handleIdempotencyKeyConflict(idempotencyKey, e, operationDao,
                responseObserver, FinishWorkflowResponse.class, checkOpResultDelay, opTimeout, LOG))
            {
                return;
            }

            LOG.error("Cannot finish workflow: { userId: {}, wfName: {}, execId: {}, error: {} }",
                userId, wfName, execId, e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withDescription("Cannot finish workflow").asRuntimeException());
            return;
        }

        if (success[0]) {
            var idk = idempotencyKey != null ? idempotencyKey.token() : null;
            try {
                LOG.debug("Schedule action to finish execution: { wfName: {}, execId: {} }", wfName, execId);
                actionsManager.finishExecutionAction(op.id(), op.description(), idk, execId,
                    Status.OK.withDescription(reason));
            } catch (Exception e) {
                LOG.error("Cannot schedule action to finish workflow: { wfName: {}, execId: {}, error: {} }", wfName,
                    execId, e.getMessage(), e);
                responseObserver.onError(
                    Status.INTERNAL.withDescription("Cannot finish workflow").asRuntimeException());
                return;
            }
        }

        responseObserver.onNext(FinishWorkflowResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void abortWorkflow(AbortWorkflowRequest request, StreamObserver<AbortWorkflowResponse> responseObserver) {
        var userId = currentSubject().id();
        var wfName = request.getWorkflowName();
        var execId = request.getExecutionId();
        var reason = request.getReason();

        LOG.info("Request to abort workflow with active execution: { userId: {}, wfName: {}, execId: {}, " +
            "reason: {} }", userId, wfName, execId, reason);

        if (validate(userId, request, responseObserver)) {
            return;
        }

        var checkOpResultDelay = Duration.ofMillis(300);
        var opTimeout = Duration.ofSeconds(15);
        Operation.IdempotencyKey idempotencyKey = IdempotencyUtils.getIdempotencyKey(request);
        if (idempotencyKey != null && loadExistingOpResult(operationDao, idempotencyKey, responseObserver,
            AbortWorkflowResponse.class, checkOpResultDelay, opTimeout, LOG))
        {
            return;
        }

        boolean[] success = {false};
        var op = Operation.create(userId, String.format("Abort workflow with active execution: userId='%s', " +
            "wfName='%s', activeExecId='%s'", userId, wfName, execId), null, idempotencyKey, null);

        try {
            withRetries(LOG, () -> {
                try (var tx = TransactionHandle.create(storage)) {
                    success[0] = workflowDao.deactivate(userId, wfName, execId, tx);
                    if (success[0]) {
                        var opsToCancel = executionOperationsDao.get(execId, tx).stream()
                            .filter(opInfo -> opInfo.type() != ExecutionOperationsDao.OpType.STOP)
                            .map(ExecutionOperationsDao.OpInfo::opId)
                            .toList();
                        if (!opsToCancel.isEmpty()) {
                            operationDao.cancel(opsToCancel, toProto(Status.CANCELLED.withDescription("Execution " +
                                "was aborted")), tx);
                        }

                        operationDao.create(op, tx);
                        executionOperationsDao.create(op.id(), instanceId, execId, new StopExecutionOperationState(),
                            tx);
                    }
                    tx.commit();
                }
            });
        } catch (NotFoundException e) {
            LOG.error("Cannot abort workflow, not found: { wfName: {}, execId: {}, error: {} }",
                wfName, execId, e.getMessage(), e);
            responseObserver.onError(Status.NOT_FOUND.withDescription(e.getMessage()).asRuntimeException());
            return;
        } catch (IllegalStateException e) {
            LOG.error("Cannot abort workflow, invalid state: { wfName: {}, execId: {}, error: {} }",
                wfName, execId, e.getMessage(), e);
            responseObserver.onError(Status.FAILED_PRECONDITION.withDescription(e.getMessage()).asRuntimeException());
            return;
        } catch (Exception e) {
            if (idempotencyKey != null && handleIdempotencyKeyConflict(idempotencyKey, e, operationDao,
                responseObserver, AbortWorkflowResponse.class, checkOpResultDelay, opTimeout, LOG))
            {
                return;
            }

            LOG.error("Unexpected error while abort workflow: { wfName: {}, execId: {}, error: {} }",
                wfName, execId, e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withDescription("Cannot abort execution " +
                "'%s': %s".formatted(execId, e.getMessage())).asRuntimeException());
            return;
        }

        if (success[0]) {
            var idk = idempotencyKey != null ? idempotencyKey.token() : null;
            try {
                LOG.debug("Schedule action to abort workflow: { wfName: {}, execId: {} }", wfName, execId);
                actionsManager.abortExecutionAction(op.id(), op.description(), idk, execId,
                    Status.CANCELLED.withDescription(reason));
            } catch (Exception e) {
                LOG.error("Cannot schedule action to abort workflow: { wfName: {}, execId: {} }", wfName, execId);
                responseObserver.onError(Status.INTERNAL.withDescription("Cannot abort workflow").asRuntimeException());
                return;
            }
        }

        responseObserver.onNext(AbortWorkflowResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void executeGraph(ExecuteGraphRequest request, StreamObserver<LongRunning.Operation> responseObserver) {
        var userId = currentSubject().id();
        var wfName = request.getWorkflowName();
        var execId = request.getExecutionId();

        LOG.info("Request to execute graph: { userId: {}, wfName: {}, execId: {} }", userId, wfName, execId);

        if (validate(userId, request, responseObserver)) {
            return;
        }

        Operation.IdempotencyKey idempotencyKey = IdempotencyUtils.getIdempotencyKey(request);
        if (idempotencyKey != null && loadExistingOp(operationDao, idempotencyKey, responseObserver, LOG)) {
            return;
        }

        var op = Operation.create(userId, "Execute graph: userId='%s', wfName='%s', activeExecId='%s'".formatted(
            userId, wfName, execId), Duration.ofSeconds(10), idempotencyKey, null);

        try (var tx = TransactionHandle.create(storage)) {
            if (!Objects.equals(workflowDao.getExecutionId(userId, wfName, tx), execId)) {
                throw new NotFoundException("Cannot found active execution with id='%s'".formatted(execId));
            }

            for (ExecutionOperationsDao.OpInfo opInfo : executionOperationsDao.get(execId, tx)) {
                if (opInfo.type() == ExecutionOperationsDao.OpType.START) {
                    throw new InvalidStateException("Execution with id='%s' is not started yet".formatted(execId));
                }
                if (opInfo.type() == ExecutionOperationsDao.OpType.STOP) {
                    throw new InvalidStateException("Execution with id='%s' is being stopped".formatted(execId));
                }
            }

            operationDao.create(op, tx);
            executionOperationsDao.create(op.id(), instanceId, execId, new ExecuteGraphOperationState(), tx);
            tx.commit();
        } catch (NotFoundException nfe) {
            LOG.error("Cannot execute graph: { execId: {}, error: {} }", execId, nfe.toString(), nfe);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(nfe.getMessage()).asRuntimeException());
            return;
        } catch (InvalidStateException ise) {
            LOG.error("Cannot execute graph: { execId: {}, error: {} }", execId, ise.toString(), ise);
            responseObserver.onError(Status.FAILED_PRECONDITION.withDescription(ise.getMessage()).asRuntimeException());
            return;
        } catch (Exception e) {
            if (idempotencyKey != null &&
                handleIdempotencyKeyConflict(idempotencyKey, e, operationDao, responseObserver, LOG))
            {
                return;
            }

            LOG.error("Cannot execute graph: { execId: {}, error: {} }", execId, e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
            return;
        }

        var idk = idempotencyKey != null ? idempotencyKey.token() : null;
        try {
            LOG.debug("Schedule execute graph action: { execId: {} }", execId);

            var poolsZone = Strings.isBlank(request.getGraph().getZone()) ? null : request.getGraph().getZone();
            var operations = request.getGraph().getOperationsList();
            var opsPoolSpec = operations.stream().map(LWF.Operation::getPoolSpecName).toList();
            var slot2description = request.getGraph().getDataDescriptionsList().stream()
                .collect(Collectors.toMap(LWF.DataDescription::getStorageUri, Function.identity()));

            actionsManager.executeGraphAction(op.id(), op.description(), idk, userId, wfName, execId, poolsZone,
                opsPoolSpec, slot2description, operations);
        } catch (Exception e) {
            LOG.error("Cannot schedule action to execute graph: { execId: {}, error: {} }", execId, e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withDescription("Cannot execute graph").asRuntimeException());
            return;
        }

        responseObserver.onNext(op.toProto());
        responseObserver.onCompleted();
    }

    @Override
    public void graphStatus(GraphStatusRequest request, StreamObserver<GraphStatusResponse> responseObserver) {
        var userId = currentSubject().id();

        if (validate(userId, request, responseObserver)) {
            return;
        }

        graphExecutionService.graphStatus(request, responseObserver);
    }

    @Override
    public void stopGraph(StopGraphRequest request, StreamObserver<StopGraphResponse> responseObserver) {
        var userId = currentSubject().id();

        if (validate(userId, request, responseObserver)) {
            return;
        }

        graphExecutionService.stopGraph(request, responseObserver);
    }

    @Override
    public void readStdSlots(ReadStdSlotsRequest request, StreamObserver<ReadStdSlotsResponse> responseObserver) {
        String userId = currentSubject().id();
        String executionId = request.getExecutionId();

        if (checkPermissionOnExecution(userId, executionId, responseObserver)) {
            return;
        }

        workflowService.readStdSlots(request, responseObserver);
    }

    @Override
    public void getAvailablePools(GetAvailablePoolsRequest request,
                                  StreamObserver<GetAvailablePoolsResponse> responseObserver)
    {
        String userId = currentSubject().id();
        String executionId = request.getExecutionId();

        if (checkPermissionOnExecution(userId, executionId, responseObserver)) {
            return;
        }

        workflowService.getAvailablePools(request, responseObserver);
    }

    @Override
    public void getOrCreateDefaultStorage(GetOrCreateDefaultStorageRequest request,
                                          StreamObserver<GetOrCreateDefaultStorageResponse> responseObserver)
    {
        final String userId = currentSubject().id();
        final String bucketName = StorageUtils.createInternalBucketName(userId);

        LOG.info("Get storage credentials for bucket {}", bucketName);
        final LMST.StorageConfig storageConfig;
        try {
            LOG.info("Creating new temporary storage bucket if it does not exist: { bucketName: {}, userId: {} }",
                bucketName, userId);
            LongRunning.Operation createOp =
                withIdempotencyKey(storagesClient, userId + "_" + bucketName)
                    .createStorage(LSS.CreateStorageRequest.newBuilder()
                        .setUserId(userId)
                        .setBucket(bucketName)
                        .build());

            createOp = awaitOperationDone(storageOpsClient, createOp.getId(), bucketCreationTimeout);
            if (!createOp.getDone()) {
                try {
                    // do not wait until op is cancelled here
                    //noinspection ResultOfMethodCallIgnored
                    storageOpsClient.cancel(
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

    private boolean checkPermissionOnExecution(String userId, String executionId,
                                               StreamObserver<? extends MessageOrBuilder> response)
    {
        try {
            if (withRetries(LOG, () -> executionDao.exists(executionId, userId, null))) {
                return false;
            }

            LOG.error("Cannot find execution of user: { execId: {}, userId: {} }", executionId, userId);
            response.onError(Status.INVALID_ARGUMENT.withDescription("Cannot find execution '%s' of user '%s'"
                .formatted(executionId, userId)).asRuntimeException());
        } catch (Exception e) {
            LOG.error("Cannot check that execution of user exists: { executionId: {}, userId: " +
                "{}, error: {} } ", executionId, userId, e.getMessage());
            response.onError(Status.INTERNAL.withDescription("Error while checking that user " +
                "has permissions on requested execution").asRuntimeException());
        }

        return true;
    }

    private void deleteTempUserBucket(String bucket) {
        if (StringUtils.isEmpty(bucket)) {
            return;
        }

        LOG.info("Deleting temp storage bucket '{}'", bucket);

        try {
            @SuppressWarnings("unused")
            var resp = storagesClient.deleteStorage(
                LSS.DeleteStorageRequest.newBuilder()
                    .setBucket(bucket)
                    .build());
        } catch (StatusRuntimeException e) {
            LOG.error("Can't delete temp bucket '{}': ({}) {}", bucket, e.getStatus(), e.getMessage(), e);
        }
    }
}
