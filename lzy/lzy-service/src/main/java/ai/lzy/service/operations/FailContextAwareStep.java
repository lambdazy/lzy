package ai.lzy.service.operations;

import ai.lzy.longrunning.OperationRunnerBase.StepResult;
import io.grpc.StatusRuntimeException;
import org.apache.logging.log4j.Logger;

import java.util.function.Function;

public interface FailContextAwareStep {
    Function<StatusRuntimeException, StepResult> failAction();

    Logger log();

    String logPrefix();
}
