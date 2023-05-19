package ai.lzy.service.operations;

import ai.lzy.common.IdGenerator;
import ai.lzy.longrunning.OperationRunnerBase.StepResult;
import ai.lzy.model.db.Storage;
import ai.lzy.service.dao.ExecutionDao;
import ai.lzy.service.dao.ExecutionOperationsDao;
import ai.lzy.service.dao.GraphDao;
import ai.lzy.service.dao.WorkflowDao;
import ai.lzy.util.grpc.ProtoPrinter;
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

    default Storage storage() {
        return stepCtx().storage();
    }

    default WorkflowDao wfDao() {
        return stepCtx().wfDao();
    }

    default ExecutionDao execDao() {
        return stepCtx().execDao();
    }

    default GraphDao graphDao() {
        return stepCtx().graphDao();
    }

    default ExecutionOperationsDao execOpsDao() {
        return stepCtx().execOpsDao();
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

    @Override
    default ProtoPrinter.Printer safePrinter() {
        return ProtoPrinter.safePrinter();
    }

    @Override
    default ProtoPrinter.Printer printer() {
        return ProtoPrinter.printer();
    }
}
