package ai.lzy.service;

import ai.lzy.longrunning.IdempotencyUtils;
import ai.lzy.longrunning.Operation;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.service.dao.ExecutionOperationsDao;
import ai.lzy.v1.workflow.LWFPS.AbortExecutionRequest;
import ai.lzy.v1.workflow.LWFPS.AbortExecutionResponse;
import ai.lzy.v1.workflow.LzyWorkflowPrivateServiceGrpc.LzyWorkflowPrivateServiceImplBase;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;

import java.time.Duration;

import static ai.lzy.longrunning.IdempotencyUtils.handleIdempotencyKeyConflict;
import static ai.lzy.longrunning.IdempotencyUtils.loadExistingOpResult;
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
    public void abortExecution(AbortExecutionRequest request, StreamObserver<AbortExecutionResponse> responseObserver) {
        var execId = request.getExecutionId();
        var reason = request.getReason();

        LOG.info("Request to abort broken execution: {}", safePrinter().printToString(request));

        if (Strings.isBlank(execId) || Strings.isBlank(reason)) {
            var errorMes = "Cannot abort execution. Blank 'execution id' or 'reason'";
            LOG.error(errorMes);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(errorMes).asRuntimeException());
            return;
        }

        var checkOpResultDelay = Duration.ofMillis(300);
        var opTimeout = Duration.ofSeconds(15);
        Operation.IdempotencyKey idempotencyKey = IdempotencyUtils.getIdempotencyKey(request);
        if (idempotencyKey != null && loadExistingOpResult(opsDao(), idempotencyKey, responseObserver,
            AbortExecutionResponse.class, checkOpResultDelay, opTimeout, LOG))
        {
            return;
        }

        var op = Operation.create("channel-manager", "Abort broken execution: execId='%s'"
            .formatted(execId), opTimeout, idempotencyKey, null);

        try {
            withRetries(LOG, () -> {
                try (var tx = TransactionHandle.create(storage())) {
                    if (wfDao().setActiveExecutionIdToNull(execId, tx)) {
                        var opsToCancel = execOpsDao().listOpsInfo(execId, tx).stream()
                            .filter(opInfo -> opInfo.type() != ExecutionOperationsDao.OpType.STOP_EXECUTION)
                            .map(ExecutionOperationsDao.OpInfo::opId)
                            .toList();
                        if (!opsToCancel.isEmpty()) {
                            opsDao().fail(opsToCancel, toProto(Status.CANCELLED.withDescription(
                                "Execution was broke and aborted")), tx);
                        }

                        opsDao().create(op, tx);
                        execOpsDao().createStopOp(op.id(), serviceCfg().getInstanceId(), execId, tx);
                    }

                    tx.commit();
                }
            });
        } catch (Exception e) {
            if (idempotencyKey != null && handleIdempotencyKeyConflict(idempotencyKey, e, opsDao(), responseObserver,
                AbortExecutionResponse.class, checkOpResultDelay, opTimeout, LOG))
            {
                return;
            }

            LOG.error("Unexpected error while abort broken execution: { execId: {}, error: {} }", execId,
                e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withDescription("Cannot abort broken execution " +
                "'%s': %s".formatted(execId, e.getMessage())).asRuntimeException());
            return;
        }

        var idk = idempotencyKey != null ? idempotencyKey.token() : null;
        try {
            LOG.info("Schedule action to abort broken execution: { execId: {} }", execId);
            var opRunner = opRunnersFactory().createAbortExecOpRunner(op.id(), op.description(), idk, null, null,
                execId, Status.CANCELLED.withDescription(reason));
            opsExecutor().startNew(opRunner);
        } catch (Exception e) {
            LOG.error("Cannot schedule action to abort broken execution: { execId: {} }", execId);
            responseObserver.onError(Status.INTERNAL.withDescription("Cannot abort broken execution")
                .asRuntimeException());
            return;
        }

        responseObserver.onNext(AbortExecutionResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }
}
