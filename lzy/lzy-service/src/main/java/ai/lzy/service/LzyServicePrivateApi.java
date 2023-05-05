package ai.lzy.service;

import ai.lzy.iam.grpc.context.AuthenticationContext;
import ai.lzy.longrunning.Operation;
import ai.lzy.longrunning.OperationsExecutor;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.NotFoundException;
import ai.lzy.service.workflow.finish.FinishExecutionAction;
import ai.lzy.v1.workflow.LWFPS;
import ai.lzy.v1.workflow.LzyWorkflowPrivateServiceGrpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.micronaut.core.util.StringUtils;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static ai.lzy.longrunning.IdempotencyUtils.handleIdempotencyKeyConflict;
import static ai.lzy.model.db.DbHelper.withRetries;

@Singleton
public class LzyServicePrivateApi extends LzyWorkflowPrivateServiceGrpc.LzyWorkflowPrivateServiceImplBase {
    private static final Logger LOG = LogManager.getLogger(LzyServicePrivateApi.class);

    private final OperationsExecutor operationsExecutor;

    public LzyServicePrivateApi(@Named("LzyServiceOperationsExecutor") OperationsExecutor operationsExecutor) {
        this.operationsExecutor = operationsExecutor;
    }

    @Override
    public void abortExecution(LWFPS.AbortExecutionRequest request,
                               StreamObserver<LWFPS.AbortExecutionResponse> response)
    {
        var userId = AuthenticationContext.currentSubject().id();
        var executionId = request.getExecutionId();
        var reason = request.getReason();

        if (StringUtils.isEmpty(executionId)) {
            LOG.error("Cannot abort execution: { executionId: {} }", executionId);
            response.onError(Status.INVALID_ARGUMENT.withDescription("Empty 'executionId'").asRuntimeException());
            return;
        }

        LOG.info("Attempt to abort execution: { userId: {}, executionId: {} }", userId, executionId);

        var op = Operation.create(userId, "Abort workflow: wfName='%s', activeExecId='%s'"
            .formatted(workflowName, executionId), null, idempotencyKey, null);
//        var finishStatus = Status.OK.withDescription(reason);

        try {
            withRetries(LOG, () -> {
                try (var tx = TransactionHandle.create(storage)) {
                    operationDao.create(op, tx);
                    workflowDao.setActiveExecutionToNull(userId, workflowName, executionId, tx);

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

        // with skipErrors
        var stopExecutionOp = new FinishExecutionAction();
        operationsExecutor.startNew(stopExecutionOp);

        response.onNext(op.toProto());
        response.onCompleted();
    }
}
