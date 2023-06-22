package ai.lzy.service.operations;

import ai.lzy.longrunning.OperationRunnerBase.StepResult;
import io.grpc.StatusRuntimeException;

import java.util.function.Function;

public interface FailContextAwareStep extends LogContextAwareStep {
    Function<StatusRuntimeException, StepResult> failAction();
}
