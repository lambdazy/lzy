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

public abstract class ExecutionContextAwareStep implements FailContextAwareStep {
    private final ExecutionStepContext stepCtx;

    public ExecutionContextAwareStep(ExecutionStepContext stepCtx) {
        this.stepCtx = stepCtx;
    }

    protected ExecutionStepContext stepCtx() {
        return stepCtx;
    }

    protected String opId() {
        return stepCtx.opId();
    }

    protected String userId() {
        return stepCtx.userId();
    }

    protected String wfName() {
        return stepCtx.wfName();
    }

    protected String execId() {
        return stepCtx.execId();
    }

    protected Storage storage() {
        return stepCtx.storage();
    }

    protected WorkflowDao wfDao() {
        return stepCtx.wfDao();
    }

    protected ExecutionDao execDao() {
        return stepCtx.execDao();
    }

    protected GraphDao graphDao() {
        return stepCtx.graphDao();
    }

    protected ExecutionOperationsDao execOpsDao() {
        return stepCtx.execOpsDao();
    }

    protected String idempotencyKey() {
        return stepCtx.idempotencyKey();
    }

    protected IdGenerator idGenerator() {
        return stepCtx.idGenerator();
    }

    @Override
    public Function<StatusRuntimeException, StepResult> failAction() {
        return stepCtx.failAction();
    }

    @Override
    public Logger log() {
        return stepCtx.log();
    }

    @Override
    public String logPrefix() {
        return stepCtx.logPrefix();
    }

    @Override
    public ProtoPrinter.Printer safePrinter() {
        return ProtoPrinter.safePrinter();
    }

    @Override
    public ProtoPrinter.Printer printer() {
        return ProtoPrinter.printer();
    }
}
