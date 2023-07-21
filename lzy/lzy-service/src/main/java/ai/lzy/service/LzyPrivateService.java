package ai.lzy.service;

import ai.lzy.longrunning.IdempotencyUtils;
import ai.lzy.longrunning.Operation;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.v1.workflow.LWFS;
import ai.lzy.v1.workflow.LzyWorkflowPrivateServiceGrpc.LzyWorkflowPrivateServiceImplBase;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;

import java.time.Duration;
import java.time.Instant;

import static ai.lzy.longrunning.IdempotencyUtils.handleIdempotencyKeyConflict;
import static ai.lzy.longrunning.IdempotencyUtils.loadExistingOpResult;
import static ai.lzy.longrunning.OperationUtils.awaitOperationDone;
import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.util.grpc.ProtoConverter.toProto;
import static ai.lzy.util.grpc.ProtoPrinter.safePrinter;

@Singleton
public class LzyPrivateService extends LzyWorkflowPrivateServiceImplBase implements ContextAwareService {
    private static final Logger LOG = LogManager.getLogger(LzyPrivateService.class);

    private final LzyServiceContext serviceContext;

    public LzyPrivateService(LzyServiceContext serviceContext) {
        this.serviceContext = serviceContext;
    }

    @Override
    public LzyServiceContext lzyServiceCtx() {
        return serviceContext;
    }

    @Override
    public void abortWorkflow(LWFS.AbortWorkflowRequest request, StreamObserver<LWFS.AbortWorkflowResponse> response) {
        var wfName = request.getWorkflowName();
        var execId = request.getExecutionId();
        var reason = request.getReason();

        var checkOpResultDelay = Duration.ofMillis(300);
        var opTimeout = Duration.ofSeconds(30);
        Operation.IdempotencyKey idempotencyKey = IdempotencyUtils.getIdempotencyKey(request);
        if (idempotencyKey != null && loadExistingOpResult(opsDao(), idempotencyKey, response,
            LWFS.AbortWorkflowResponse.class, checkOpResultDelay, opTimeout, "Request to abort workflow: %s"
                .formatted(safePrinter().shortDebugString(request)), LOG))
        {
            return;
        }

        LOG.info("Request to abort workflow: { idempotencyKey: {}, request: {} }",
            idempotencyKey != null ? idempotencyKey.token() : "null", safePrinter().shortDebugString(request));

        if (Strings.isBlank(wfName) || Strings.isBlank(execId) || Strings.isBlank(reason)) {
            var errorMes = "Cannot abort workflow. Blank 'workflow name' or 'execution id' or 'reason'";
            LOG.error(errorMes);
            response.onError(Status.INVALID_ARGUMENT.withDescription(errorMes).asRuntimeException());
            return;
        }

        var op = Operation.create("channel-manager", ("Abort workflow with active execution: wfName='%s', " +
            "activeExecId='%s'").formatted(wfName, execId), opTimeout, idempotencyKey, null);
        try {
            withRetries(LOG, () -> {
                try (var tx = TransactionHandle.create(storage())) {
                    if (wfDao().cleanActiveExecutionById(wfName, execId, tx)) {
                        var opsToCancel = execOpsDao().listOpsIdsToCancel(execId, tx);
                        if (!opsToCancel.isEmpty()) {
                            execOpsDao().deleteOps(opsToCancel, tx);
                            opsDao().fail(opsToCancel, toProto(Status.CANCELLED.withDescription(
                                "Execution was aborted")), tx);
                        }

                        opsDao().create(op, tx);
                        execOpsDao().createAbortOp(op.id(), serviceCfg().getInstanceId(), execId, tx);
                        execDao().setFinishStatus(execId, Status.CANCELLED.withDescription(reason), tx);
                    } else {
                        throw new IllegalStateException("Execution with id='%s' is not an active".formatted(execId));
                    }

                    tx.commit();
                }
            });
        } catch (IllegalStateException ise) {
            LOG.error("Cannot abort workflow: { wfName: {}, error: {} }", wfName, ise.getMessage(), ise);
            response.onError(Status.ABORTED.withDescription("Cannot abort workflow: " + ise.getMessage())
                .asRuntimeException());
            return;
        } catch (Exception e) {
            if (idempotencyKey != null && handleIdempotencyKeyConflict(idempotencyKey, e, opsDao(), response,
                LWFS.AbortWorkflowResponse.class, checkOpResultDelay, opTimeout, LOG))
            {
                return;
            }

            LOG.error("Unexpected error while abort broken execution: { execId: {}, error: {} }", execId,
                e.getMessage(), e);
            response.onError(Status.INTERNAL.withDescription("Cannot abort broken execution " +
                "'%s': %s".formatted(execId, e.getMessage())).asRuntimeException());
            return;
        }

        var idk = idempotencyKey != null ? idempotencyKey.token() : null;
        try {
            LOG.info("Schedule action to abort workflow: { wfName: {}, execId: {} }", wfName, execId);
            var opRunner = opRunnersFactory().createAbortExecOpRunner(op.id(), op.description(), idk, null, wfName,
                execId);
            opsExecutor().startNew(opRunner);
        } catch (Exception e) {
            LOG.error("Cannot schedule action to abort workflow: { wfName: {}, execId: {} }", wfName, execId);
            response.onError(Status.INTERNAL.withDescription("Cannot abort workflow").asRuntimeException());
            return;
        }

        Operation operation = awaitOperationDone(opsDao(), op.id(), checkOpResultDelay, opTimeout, LOG);
        if (operation == null) {
            LOG.error("Unexpected operation dao state: abort execution operation with id='{}' not found", op.id());
            response.onError(Status.INTERNAL.withDescription("Cannot abort execution").asRuntimeException());
        } else if (!operation.done()) {
            LOG.error("Abort execution operation with id='{}' not completed in time", op.id());
            response.onError(Status.DEADLINE_EXCEEDED.withDescription("Cannot abort execution")
                .asRuntimeException());
        } else if (operation.response() != null) {
            try {
                response.onNext(operation.response().unpack(LWFS.AbortWorkflowResponse.class));
                response.onCompleted();
            } catch (InvalidProtocolBufferException e) {
                LOG.error("Unexpected result of abort execution operation: {}", e.getMessage(), e);
                response.onError(Status.INTERNAL.withDescription("Cannot abort execution")
                    .asRuntimeException());
            }
        } else {
            response.onError(operation.error().asRuntimeException());
        }
    }
}
