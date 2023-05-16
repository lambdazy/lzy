package ai.lzy.service.workflow;

import ai.lzy.common.IdGenerator;
import ai.lzy.longrunning.OperationRunnerBase.StepResult;
import ai.lzy.service.dao.ExecutionDao;
import ai.lzy.service.dao.WorkflowDao;
import io.grpc.StatusRuntimeException;
import org.apache.logging.log4j.Logger;

import java.util.function.Function;

public interface ExecutionContextAwareStep extends FailContextAwareStep {
    ExecutionStepContext stepCtx();

    default String opId() {
        return stepCtx().opId();
    }

    default String userId() {
        return stepCtx().userId();
    }

    default String wfName() {
        return stepCtx().wfName();
    }

    default String execId() {
        return stepCtx().execId();
    }

    default WorkflowDao wfDao() {
        return stepCtx().wfDao();
    }

    default ExecutionDao execDao() {
        return stepCtx().execDao();
    }

    default String idempotencyKey() {
        return stepCtx().idempotencyKey();
    }

    default IdGenerator idGenerator() {
        return stepCtx().idGenerator();
    }

    @Override
    default Function<StatusRuntimeException, StepResult> failAction() {
        return stepCtx().failAction();
    }

    @Override
    default Logger log() {
        return stepCtx().log();
    }

    @Override
    default String logPrefix() {
        return stepCtx().logPrefix();
    }
}
