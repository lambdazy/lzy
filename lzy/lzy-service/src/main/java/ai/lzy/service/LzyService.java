package ai.lzy.service;

import ai.lzy.longrunning.IdempotencyUtils;
import ai.lzy.longrunning.Operation;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.NotFoundException;
import ai.lzy.service.dao.ExecuteGraphState;
import ai.lzy.service.dao.ExecutionOperationsDao;
import ai.lzy.service.dao.ExecutionOperationsDao.ExecutionOpState;
import ai.lzy.service.dao.ExecutionOperationsDao.OpType;
import ai.lzy.service.util.ProtoConverter;
import ai.lzy.service.util.StorageUtils;
import ai.lzy.util.grpc.GrpcHeaders;
import ai.lzy.v1.VmPoolServiceApi;
import ai.lzy.v1.common.LMST;
import ai.lzy.v1.graph.GraphExecutorApi;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.storage.LSS;
import ai.lzy.v1.workflow.LWFS;
import ai.lzy.v1.workflow.LzyWorkflowServiceGrpc;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static ai.lzy.iam.grpc.context.AuthenticationContext.currentSubject;
import static ai.lzy.longrunning.IdempotencyUtils.handleIdempotencyKeyConflict;
import static ai.lzy.longrunning.IdempotencyUtils.loadExistingOpResult;
import static ai.lzy.longrunning.OperationGrpcServiceUtils.awaitOperationDone;
import static ai.lzy.longrunning.OperationUtils.awaitOperationDone;
import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.util.grpc.GrpcUtils.*;
import static ai.lzy.util.grpc.ProtoConverter.toProto;
import static ai.lzy.util.grpc.ProtoPrinter.safePrinter;
import static ai.lzy.v1.workflow.LWFS.*;

@Singleton
public class LzyService extends LzyWorkflowServiceGrpc.LzyWorkflowServiceImplBase implements ContextAwareService {
    private static final Logger LOG = LogManager.getLogger(LzyService.class);
    public static final String APP = "LzyService";

    private final LzyServiceContext serviceContext;

    public LzyService(LzyServiceContext serviceContext) {
        this.serviceContext = serviceContext;
        restartNotCompletedOps();
    }

    private void restartNotCompletedOps() {
        var serviceInstanceId = serviceCfg().getInstanceId();

        List<ExecutionOpState> uncompletedOps;
        try {
            uncompletedOps = withRetries(LOG, () -> execOpsDao().listUncompletedOps(serviceInstanceId, null));
        } catch (Exception e) {
            LOG.error("Lzy service instance with id='{}' cannot load uncompleted operation from dao: {}",
                serviceInstanceId, e.getMessage(), e);
            throw new RuntimeException("Cannot start lzy service instance", e);
        }

        LOG.debug("Lzy service instance with id='{}' found {} uncompleted operations", serviceInstanceId,
            uncompletedOps.size());

        for (var op : uncompletedOps) {
            try {
                var actionRunner = switch (op.type()) {
                    case START_EXECUTION -> opRunnersFactory().createStartExecOpRunner(
                        op.opId(), op.opDesc(), op.idempotencyKey(), op.userId(), op.wfName(),
                        op.execId());
                    case FINISH_EXECUTION -> opRunnersFactory().createFinishExecOpRunner(
                        op.opId(), op.opDesc(), op.idempotencyKey(), op.userId(), op.wfName(), op.execId());
                    case ABORT_EXECUTION -> opRunnersFactory().createAbortExecOpRunner(
                        op.opId(), op.opDesc(), op.idempotencyKey(), op.userId(), op.wfName(), op.execId());
                    case EXECUTE_GRAPH -> opRunnersFactory().createExecuteGraphOpRunner(
                        op.opId(), op.opDesc(), op.idempotencyKey(), op.userId(), op.wfName(), op.execId());
                };
                opsExecutor().startNew(actionRunner);
            } catch (Exception e) {
                LOG.error("Lzy service instance with id='{}' cannot reschedule some uncompleted operation: {}",
                    serviceInstanceId, e.getMessage(), e);
                throw new RuntimeException("Cannot start lzy service instance", e);
            }
        }
    }

    @Override
    public LzyServiceContext lzyServiceCtx() {
        return serviceContext;
    }

    @Override
    public void startWorkflow(StartWorkflowRequest request, StreamObserver<StartWorkflowResponse> responseObserver) {
        var userId = currentSubject().id();
        var wfName = request.getWorkflowName();
        var storageName = request.getStorageName();
        var storageCfg = request.getSnapshotStorage();
        var newExecId = Objects.requireNonNull(GrpcHeaders.getExecutionId());

        var checkOpResultDelay = Duration.ofMillis(300);
        var startOpTimeout = serviceCfg().getOperations().getStartWorkflowTimeout();
        Operation.IdempotencyKey idempotencyKey = IdempotencyUtils.getIdempotencyKey(request);
        if (idempotencyKey != null && loadExistingOpResult(opsDao(), idempotencyKey, responseObserver,
            StartWorkflowResponse.class, checkOpResultDelay, startOpTimeout, "Request to start workflow: %s"
                .formatted(request), LOG))
        {
            return;
        }

        LOG.info("Request to start workflow execution: { idempotencyKey: {}, request: {} }",
            idempotencyKey != null ? idempotencyKey.token() : "null", safePrinter().shortDebugString(request));

        if (validator().validate(request, responseObserver)) {
            return;
        }

        Operation startOp = Operation.create(userId, ("Start workflow execution: userId='%s', wfName='%s', " +
            "newActiveExecId=%s").formatted(userId, wfName, newExecId), startOpTimeout, idempotencyKey, null);
        Operation abortOp = null;
        String oldExecId;

        try (var tx = TransactionHandle.create(storage())) {
            execDao().create(userId, newExecId, storageName, storageCfg, tx);
            oldExecId = wfDao().upsert(userId, wfName, newExecId, tx);

            if (oldExecId != null) {
                abortOp = Operation.create(userId, "Abort previous execution: userId='%s', wfName='%s', execId='%s'"
                    .formatted(userId, wfName, oldExecId), serviceCfg().getOperations().getAbortWorkflowTimeout(),
                    null, null);
                opsDao().create(abortOp, tx);
                execOpsDao().createAbortOp(abortOp.id(), serviceCfg().getInstanceId(), oldExecId, tx);
                execDao().setFinishStatus(oldExecId, Status.CANCELLED.withDescription("by new started execution"), tx);
            }

            opsDao().create(startOp, tx);
            execOpsDao().createStartOp(startOp.id(), serviceCfg().getInstanceId(), newExecId, tx);
            tx.commit();
        } catch (Exception e) {
            if (idempotencyKey != null && handleIdempotencyKeyConflict(idempotencyKey, e, opsDao(), responseObserver,
                StartWorkflowResponse.class, checkOpResultDelay, startOpTimeout, LOG))
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
            LOG.info("Schedule action to start execution: { userId: {}, wfName: {} }", userId, wfName);
            var opRunner = opRunnersFactory().createStartExecOpRunner(startOp.id(), startOp.description(), idk, userId,
                wfName, newExecId);
            opsExecutor().startNew(opRunner);
        } catch (Exception e) {
            LOG.error("Cannot schedule action to start new workflow execution: { userId: {}, wfName: {}, error: {} } ",
                userId, wfName, e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withDescription("Cannot start workflow").asRuntimeException());
            return;
        }
        try {
            if (abortOp != null) {
                LOG.info("Schedule action to abort previous execution: { userId: {}, wfName: {}, prevExecId: {} }",
                    userId, wfName, oldExecId);
                var opRunner = opRunnersFactory().createAbortExecOpRunner(abortOp.id(), abortOp.description(),
                    idk, userId, wfName, oldExecId);
                opsExecutor().startNew(opRunner);
            }
        } catch (Exception e) {
            LOG.error("Cannot schedule action to abort previous active execution: { userId: {}, wfName: {}, " +
                "prevExecId: {}, error: {} }", userId, wfName, oldExecId, e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withDescription("Cannot start workflow").asRuntimeException());
            return;
        }

        Operation operation = awaitOperationDone(opsDao(), startOp.id(), checkOpResultDelay, startOpTimeout, LOG);
        if (operation == null) {
            LOG.error("Unexpected operation dao state: start workflow operation with id='{}' not found", startOp.id());
            responseObserver.onError(Status.INTERNAL.withDescription("Cannot start workflow").asRuntimeException());
        } else if (!operation.done()) {
            LOG.error("Start workflow operation with id='{}' not completed in time", startOp.id());
            responseObserver.onError(Status.DEADLINE_EXCEEDED.withDescription("Cannot start workflow")
                .asRuntimeException());
        } else if (operation.response() != null) {
            try {
                responseObserver.onNext(operation.response().unpack(StartWorkflowResponse.class));
                responseObserver.onCompleted();
            } catch (InvalidProtocolBufferException e) {
                LOG.error("Unexpected result of start workflow operation: {}", e.getMessage(), e);
                responseObserver.onError(Status.INTERNAL.withDescription("Cannot start workflow").asRuntimeException());
            }
        } else {
            responseObserver.onError(operation.error().asRuntimeException());
        }
    }

    @Override
    public void finishWorkflow(FinishWorkflowRequest request, StreamObserver<FinishWorkflowResponse> responseObserver) {
        var userId = currentSubject().id();
        var wfName = request.getWorkflowName();
        var execId = request.getExecutionId();
        var reason = request.getReason();

        var checkOpResultDelay = Duration.ofMillis(300);
        var opTimeout = serviceCfg().getOperations().getFinishWorkflowTimeout();
        Operation.IdempotencyKey idempotencyKey = IdempotencyUtils.getIdempotencyKey(request);
        if (idempotencyKey != null && loadExistingOpResult(opsDao(), idempotencyKey, responseObserver,
            FinishWorkflowResponse.class, checkOpResultDelay, opTimeout, "Request to finish workflow: %s"
                .formatted(safePrinter().shortDebugString(request)), LOG))
        {
            return;
        }

        LOG.info("Request to finish workflow: { idempotencyKey: {}, request: {} }",
            idempotencyKey != null ? idempotencyKey.token() : "null", safePrinter().shortDebugString(request));

        if (validator().validate(userId, request, responseObserver)) {
            return;
        }

        var op = Operation.create(userId, ("Finish workflow with active execution: userId='%s', wfName='%s', " +
            "activeExecId='%s'").formatted(userId, wfName, execId), opTimeout, idempotencyKey, null);

        try {
            withRetries(LOG, () -> {
                try (var tx = TransactionHandle.create(storage())) {
                    if (!Objects.equals(wfDao().getExecutionId(userId, wfName, tx), execId)) {
                        throw new IllegalStateException("Execution with id='%s' is not an active".formatted(execId));
                    }
                    wfDao().setActiveExecutionId(userId, wfName, null, tx);

                    var opsToCancel = execOpsDao().listOpsIdsToCancel(execId, tx);
                    if (!opsToCancel.isEmpty()) {
                        execOpsDao().deleteOps(opsToCancel, tx);
                        opsDao().fail(opsToCancel, toProto(Status.CANCELLED.withDescription("Execution was finished")),
                            tx);
                    }

                    opsDao().create(op, tx);
                    execOpsDao().createFinishOp(op.id(), serviceCfg().getInstanceId(), execId, tx);
                    execDao().setFinishStatus(execId, Status.OK.withDescription(reason), tx);

                    tx.commit();
                }
            });
        } catch (NotFoundException nfe) {
            LOG.error("Cannot finish workflow: { userId: {}, wfName: {}, error: {} }", userId, wfName,
                nfe.getMessage(), nfe);
            responseObserver.onError(Status.NOT_FOUND.withDescription("Cannot finish workflow: " + nfe.getMessage())
                .asRuntimeException());
            return;
        } catch (IllegalStateException ise) {
            LOG.error("Cannot finish workflow: { userId: {}, wfName: {}, error: {} }", userId, wfName,
                ise.getMessage(), ise);
            responseObserver.onError(Status.ABORTED.withDescription("Cannot finish workflow: " + ise.getMessage())
                .asRuntimeException());
            return;
        } catch (Exception e) {
            if (idempotencyKey != null && handleIdempotencyKeyConflict(idempotencyKey, e, opsDao(), responseObserver,
                FinishWorkflowResponse.class, checkOpResultDelay, opTimeout, LOG))
            {
                return;
            }

            LOG.error("Cannot finish workflow: { userId: {}, wfName: {}, execId: {}, error: {} }",
                userId, wfName, execId, e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withDescription("Cannot finish workflow").asRuntimeException());
            return;
        }

        var idk = idempotencyKey != null ? idempotencyKey.token() : null;
        try {
            LOG.info("Schedule action to finish execution: { wfName: {}, execId: {} }", wfName, execId);
            var opRunner = opRunnersFactory().createFinishExecOpRunner(op.id(), op.description(), idk, userId, wfName,
                execId);
            opsExecutor().startNew(opRunner);
        } catch (Exception e) {
            LOG.error("Cannot schedule action to finish workflow: { wfName: {}, execId: {}, error: {} }", wfName,
                execId, e.getMessage(), e);
            responseObserver.onError(
                Status.INTERNAL.withDescription("Cannot finish workflow").asRuntimeException());
            return;
        }

        Operation operation = awaitOperationDone(opsDao(), op.id(), checkOpResultDelay, opTimeout, LOG);
        if (operation == null) {
            LOG.error("Unexpected operation dao state: finish workflow operation with id='{}' not found", op.id());
            responseObserver.onError(Status.INTERNAL.withDescription("Cannot finish workflow").asRuntimeException());
        } else if (!operation.done()) {
            LOG.error("Finish workflow operation with id='{}' not completed in time", op.id());
            responseObserver.onError(Status.DEADLINE_EXCEEDED.withDescription("Cannot finish workflow")
                .asRuntimeException());
        } else if (operation.response() != null) {
            try {
                responseObserver.onNext(operation.response().unpack(FinishWorkflowResponse.class));
                responseObserver.onCompleted();
            } catch (InvalidProtocolBufferException e) {
                LOG.error("Unexpected result of finish workflow operation: {}", e.getMessage(), e);
                responseObserver.onError(Status.INTERNAL.withDescription("Cannot finish workflow")
                    .asRuntimeException());
            }
        } else {
            responseObserver.onError(operation.error().asRuntimeException());
        }
    }

    @Override
    public void abortWorkflow(AbortWorkflowRequest request, StreamObserver<AbortWorkflowResponse> responseObserver) {
        var userId = currentSubject().id();
        var wfName = request.getWorkflowName();
        var execId = request.getExecutionId();
        var reason = request.getReason();

        var checkOpResultDelay = Duration.ofMillis(300);
        var opTimeout = serviceCfg().getOperations().getAbortWorkflowTimeout();
        Operation.IdempotencyKey idempotencyKey = IdempotencyUtils.getIdempotencyKey(request);
        if (idempotencyKey != null && loadExistingOpResult(opsDao(), idempotencyKey, responseObserver,
            AbortWorkflowResponse.class, checkOpResultDelay, opTimeout, "Request to abort workflow: %s"
                .formatted(safePrinter().shortDebugString(request)), LOG))
        {
            return;
        }

        LOG.info("Request to abort workflow: { idempotencyKey: {}, request: {} }",
            idempotencyKey != null ? idempotencyKey.token() : "null", safePrinter().shortDebugString(request));

        if (validator().validate(userId, request, responseObserver)) {
            return;
        }

        var op = Operation.create(userId, ("Abort workflow with active execution: userId='%s', " +
            "wfName='%s', activeExecId='%s'").formatted(userId, wfName, execId), opTimeout, idempotencyKey, null);

        try {
            withRetries(LOG, () -> {
                try (var tx = TransactionHandle.create(storage())) {
                    if (!Objects.equals(wfDao().getExecutionId(userId, wfName, tx), execId)) {
                        throw new IllegalStateException("Execution with id='%s' is not an active".formatted(execId));
                    }
                    wfDao().setActiveExecutionId(userId, wfName, null, tx);

                    var opsToCancel = execOpsDao().listOpsIdsToCancel(execId, tx);
                    if (!opsToCancel.isEmpty()) {
                        execOpsDao().deleteOps(opsToCancel, tx);
                        opsDao().fail(opsToCancel, toProto(Status.CANCELLED.withDescription("Execution was aborted")),
                            tx);
                    }

                    opsDao().create(op, tx);
                    execOpsDao().createAbortOp(op.id(), serviceCfg().getInstanceId(), execId, tx);
                    execDao().setFinishStatus(execId, Status.CANCELLED.withDescription(reason), tx);

                    tx.commit();
                }
            });
        } catch (NotFoundException nfe) {
            LOG.error("Cannot abort workflow: { userId: {}, wfName: {}, error: {} }", userId, wfName,
                nfe.getMessage(), nfe);
            responseObserver.onError(Status.NOT_FOUND.withDescription("Cannot abort workflow: " + nfe.getMessage())
                .asRuntimeException());
            return;
        } catch (IllegalStateException ise) {
            LOG.error("Cannot abort workflow: { userId: {}, wfName: {}, error: {} }", userId, wfName,
                ise.getMessage(), ise);
            responseObserver.onError(Status.ABORTED.withDescription("Cannot abort workflow: " + ise.getMessage())
                .asRuntimeException());
            return;
        } catch (Exception e) {
            if (idempotencyKey != null && handleIdempotencyKeyConflict(idempotencyKey, e, opsDao(), responseObserver,
                AbortWorkflowResponse.class, checkOpResultDelay, opTimeout, LOG))
            {
                return;
            }

            LOG.error("Unexpected error while abort workflow: { wfName: {}, execId: {}, error: {} }",
                wfName, execId, e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withDescription("Cannot abort execution " +
                "'%s': %s".formatted(execId, e.getMessage())).asRuntimeException());
            return;
        }

        var idk = idempotencyKey != null ? idempotencyKey.token() : null;
        try {
            LOG.info("Schedule action to abort workflow: { wfName: {}, execId: {} }", wfName, execId);
            var opRunner = opRunnersFactory().createAbortExecOpRunner(op.id(), op.description(), idk, userId,
                wfName, execId);
            opsExecutor().startNew(opRunner);
        } catch (Exception e) {
            LOG.error("Cannot schedule action to abort workflow: { wfName: {}, execId: {} }", wfName, execId);
            responseObserver.onError(Status.INTERNAL.withDescription("Cannot abort workflow").asRuntimeException());
            return;
        }

        Operation operation = awaitOperationDone(opsDao(), op.id(), checkOpResultDelay, opTimeout, LOG);
        if (operation == null) {
            LOG.error("Unexpected operation dao state: abort workflow operation with id='{}' not found", op.id());
            responseObserver.onError(Status.INTERNAL.withDescription("Cannot abort workflow").asRuntimeException());
        } else if (!operation.done()) {
            LOG.error("Abort workflow operation with id='{}' not completed in time", op.id());
            responseObserver.onError(Status.DEADLINE_EXCEEDED.withDescription("Cannot abort workflow")
                .asRuntimeException());
        } else if (operation.response() != null) {
            try {
                responseObserver.onNext(operation.response().unpack(AbortWorkflowResponse.class));
                responseObserver.onCompleted();
            } catch (InvalidProtocolBufferException e) {
                LOG.error("Unexpected result of abort workflow operation: {}", e.getMessage(), e);
                responseObserver.onError(Status.INTERNAL.withDescription("Cannot abort workflow").asRuntimeException());
            }
        } else {
            responseObserver.onError(operation.error().asRuntimeException());
        }
    }

    @Override
    public void executeGraph(ExecuteGraphRequest request, StreamObserver<ExecuteGraphResponse> responseObserver) {
        var userId = currentSubject().id();
        var wfName = request.getWorkflowName();
        var execId = request.getExecutionId();

        var checkOpResultDelay = Duration.ofMillis(300);
        var opTimeout = serviceCfg().getOperations().getExecuteGraphTimeout();
        Operation.IdempotencyKey idempotencyKey = IdempotencyUtils.getIdempotencyKey(request);
        if (idempotencyKey != null && loadExistingOpResult(opsDao(), idempotencyKey, responseObserver,
            ExecuteGraphResponse.class, checkOpResultDelay, opTimeout, "Request to execute graph: %s"
                .formatted(safePrinter().printToString(request)), LOG))
        {
            return;
        }

        LOG.info("Request to execute graph: { idempotencyKey: {}, request: {} }",
            idempotencyKey != null ? idempotencyKey.token() : "null", safePrinter().printToString(request));

        if (validator().validate(userId, request, responseObserver)) {
            return;
        }

        var op = Operation.create(userId, "Execute graph: userId='%s', wfName='%s', activeExecId='%s'".formatted(
            userId, wfName, execId), opTimeout, idempotencyKey, null);

        try (var tx = TransactionHandle.create(storage())) {
            if (!Objects.equals(wfDao().getExecutionId(userId, wfName, tx), execId)) {
                throw new IllegalStateException("Execution with id='%s' is not an active".formatted(execId));
            }

            for (ExecutionOperationsDao.OpInfo opInfo : execOpsDao().listOpsInfo(execId, tx)) {
                if (opInfo.type() == OpType.START_EXECUTION) {
                    throw new IllegalStateException("Execution with id='%s' is not started yet".formatted(execId));
                }
                if (OpType.isStop(opInfo.type())) {
                    throw new IllegalStateException("Execution with id='%s' is being stopped".formatted(execId));
                }
            }

            opsDao().create(op, tx);
            execOpsDao().createExecGraphOp(op.id(), serviceCfg().getInstanceId(), execId,
                new ExecuteGraphState(request.getGraph()), tx);
            tx.commit();
        } catch (NotFoundException nfe) {
            LOG.error("Cannot execute graph: { execId: {}, error: {} }", execId, nfe.getMessage(), nfe);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(nfe.getMessage()).asRuntimeException());
            return;
        } catch (IllegalStateException ise) {
            LOG.error("Cannot execute graph: { execId: {}, error: {} }", execId, ise.getMessage(), ise);
            responseObserver.onError(Status.ABORTED.withDescription(ise.getMessage()).asRuntimeException());
            return;
        } catch (Exception e) {
            if (idempotencyKey != null &&
                handleIdempotencyKeyConflict(idempotencyKey, e, opsDao(), responseObserver,
                    ExecuteGraphResponse.class, checkOpResultDelay, opTimeout, LOG))
            {
                return;
            }

            LOG.error("Cannot execute graph: { execId: {}, error: {} }", execId, e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
            return;
        }

        var idk = idempotencyKey != null ? idempotencyKey.token() : null;
        try {
            LOG.info("Schedule action to execute graph: { execId: {} }", execId);
            var opRunner = opRunnersFactory().createExecuteGraphOpRunner(op.id(), op.description(), idk, userId, wfName,
                execId);
            opsExecutor().startNew(opRunner);
        } catch (Exception e) {
            LOG.error("Cannot schedule action to execute graph: { execId: {}, error: {} }", execId, e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withDescription("Cannot execute graph").asRuntimeException());
            return;
        }

        Operation operation = awaitOperationDone(opsDao(), op.id(), checkOpResultDelay, opTimeout, LOG);
        if (operation == null) {
            LOG.error("Unexpected operation dao state: execute graph operation with id='{}' not found", op.id());
            responseObserver.onError(Status.INTERNAL.withDescription("Cannot execute graph").asRuntimeException());
        } else if (!operation.done()) {
            LOG.error("Execute graph operation with id='{}' not completed in time", op.id());
            responseObserver.onError(Status.DEADLINE_EXCEEDED.withDescription("Cannot execute graph")
                .asRuntimeException());
        } else if (operation.response() != null) {
            try {
                responseObserver.onNext(operation.response().unpack(ExecuteGraphResponse.class));
                responseObserver.onCompleted();
            } catch (InvalidProtocolBufferException e) {
                LOG.error("Unexpected result of execute graph operation: {}", e.getMessage(), e);
                responseObserver.onError(Status.INTERNAL.withDescription("Cannot execute graph").asRuntimeException());
            }
        } else {
            responseObserver.onError(operation.error().asRuntimeException());
        }
    }

    @Override
    public void graphStatus(GraphStatusRequest request, StreamObserver<GraphStatusResponse> responseObserver) {
        var userId = currentSubject().id();
        var execId = request.getExecutionId();
        var graphId = request.getGraphId();

        LOG.debug("Request to graph status: {}", safePrinter().shortDebugString(request));

        if (validator().validate(userId, request, responseObserver)) {
            return;
        }

        GraphExecutorApi.GraphStatusResponse graphStatus;
        try {
            graphStatus = graphsGrpcClient().status(GraphExecutorApi.GraphStatusRequest.newBuilder()
                .setWorkflowId(execId).setGraphId(graphId).build());
        } catch (StatusRuntimeException e) {
            var causeStatus = e.getStatus();
            LOG.error("Cannot get graph status: { userId: {}, execId: {}, graphId: {} }, error: {}",
                userId, execId, graphId, causeStatus.getDescription(), e);
            responseObserver.onError(causeStatus.withDescription("Cannot get graph status: " +
                causeStatus.getDescription()).asRuntimeException());
            return;
        }

        if (!graphStatus.hasStatus()) {
            LOG.error("Empty graph status for graph: { userId: {}, execId: {}, graphId: {} }", userId, execId, graphId);
            responseObserver.onError(Status.INTERNAL.withDescription("Empty graph status for graph")
                .asRuntimeException());
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
                        LOG.error("Empty task status: { userId: {}, execId: {}, graphId: {}, taskId: {} }", userId,
                            execId, graphId, taskId);
                        responseObserver.onError(Status.INTERNAL.withDescription("Empty status of task with ID: " +
                            taskId).asRuntimeException());
                        return;
                    }

                    switch (status.getProgress().getStatusCase()) {
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
                .setDescription(graphStatus.getStatus().getFailed().getDescription())
                .setFailedTaskId(graphStatus.getStatus().getFailed().getFailedTaskId())
                .setFailedTaskName(graphStatus.getStatus().getFailed().getFailedTaskName()));
        }

        responseObserver.onNext(graphStatusResponse.build());
        responseObserver.onCompleted();
    }

    @Override
    public void stopGraph(StopGraphRequest request, StreamObserver<StopGraphResponse> responseObserver) {
        var userId = currentSubject().id();
        var execId = request.getExecutionId();
        var graphId = request.getGraphId();

        LOG.info("Request to stop graph: {}", safePrinter().shortDebugString(request));

        if (validator().validate(userId, request, responseObserver)) {
            return;
        }

        try {
            //noinspection ResultOfMethodCallIgnored
            graphsGrpcClient().stop(GraphExecutorApi.GraphStopRequest.newBuilder().setWorkflowId(execId)
                .setGraphId(graphId).build());
        } catch (StatusRuntimeException e) {
            var causeStatus = e.getStatus();
            LOG.error("Cannot stop graph: { userId: {}, execId: {}, graphId: {} }, error: {}",
                userId, execId, graphId, causeStatus.getDescription());
            responseObserver.onError(causeStatus.withDescription("Cannot stop graph: " + causeStatus.getDescription())
                .asRuntimeException());
            return;
        }

        responseObserver.onNext(StopGraphResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void readStdSlots(ReadStdSlotsRequest request, StreamObserver<ReadStdSlotsResponse> responseObserver) {
        var userId = currentSubject().id();
        var execId = request.getExecutionId();

        LOG.info("Request to stream std slots content for execution: {}", safePrinter().shortDebugString(request));

        if (validator().validate(userId, request, responseObserver)) {
            return;
        }

        try {
            var topicDesc = withRetries(LOG, () -> execDao().getKafkaTopicDesc(execId, null));
            if (topicDesc == null) {
                LOG.error("Null kafka topic description of execution: { execId: {} }", execId);
                responseObserver.onError(Status.FAILED_PRECONDITION.withDescription(
                    "Cannot obtain kafka source for stdout/stderr of execution").asRuntimeException());
                return;
            }
            kafkaLogsListeners().listen(request, responseObserver, topicDesc);
        } catch (Exception e) {
            LOG.error("Error while reading std slots for execution: { execId: {}, error: {} }", execId,
                e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
        }
    }

    @Override
    public void getAvailablePools(GetAvailablePoolsRequest request,
                                  StreamObserver<GetAvailablePoolsResponse> responseObserver)
    {
        var userId = currentSubject().id();

        LOG.info("Request to get available user VM pools: {}", safePrinter().shortDebugString(request));

        if (validator().validate(userId, request, responseObserver)) {
            LOG.error("Cannot get available VM pools because of invalid request: {}",
                safePrinter().printToString(request));
            return;
        }

        final List<VmPoolServiceApi.VmPoolSpec> userVmPools;
        try {
            userVmPools = vmPoolGrpcClient().getVmPools(VmPoolServiceApi.GetVmPoolsRequest.newBuilder()
                .setWithSystemPools(false).setWithUserPools(true).build()).getUserPoolsList();
        } catch (StatusRuntimeException sre) {
            LOG.error("Error in VmPoolClient:getVmPools call: {}", sre.getMessage(), sre);
            responseObserver.onError(sre);
            return;
        }

        responseObserver.onNext(GetAvailablePoolsResponse.newBuilder().addAllPoolSpecs(
            userVmPools.stream().map(ProtoConverter::to).toList()).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getOrCreateDefaultStorage(GetOrCreateDefaultStorageRequest request,
                                          StreamObserver<GetOrCreateDefaultStorageResponse> responseObserver)
    {
        final String userId = currentSubject().id();
        final String bucketName = StorageUtils.createInternalBucketName(userId);

        var idempotencyKey = IdempotencyUtils.getIdempotencyKey(request);

        var client = idempotencyKey == null
            ? storagesGrpcClient()
            : withIdempotencyKey(storagesGrpcClient(), idempotencyKey.token() + "_create_storage");

        LOG.info("Get storage credentials for bucket {}", bucketName);
        final LMST.StorageConfig storageConfig;
        try {
            LOG.info("Creating new temporary storage bucket if it does not exist: { bucketName: {}, userId: {} }",
                bucketName, userId);

            LongRunning.Operation createOp = client.createStorage(LSS.CreateStorageRequest.newBuilder()
                .setUserId(userId)
                .setBucket(bucketName)
                .build());

            createOp = awaitOperationDone(storagesOpsGrpcClient(), createOp.getId(),
                serviceCfg().getStorage().getBucketCreationTimeout());
            if (!createOp.getDone()) {
                try {
                    // do not wait until op is cancelled here
                    //noinspection ResultOfMethodCallIgnored
                    storagesOpsGrpcClient().cancel(LongRunning.CancelOperationRequest.newBuilder()
                        .setOperationId(createOp.getId()).build());
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

    private void deleteTempUserBucket(String bucket) {
        if (Strings.isBlank(bucket)) {
            return;
        }

        LOG.info("Deleting temp storage bucket '{}'", bucket);

        try {
            @SuppressWarnings("unused")
            var resp = storagesGrpcClient().deleteStorage(LSS.DeleteStorageRequest.newBuilder().setBucket(bucket)
                .build());
        } catch (StatusRuntimeException e) {
            LOG.error("Can't delete temp bucket '{}': ({}) {}", bucket, e.getStatus(), e.getMessage(), e);
        }
    }
}
