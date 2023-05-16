package ai.lzy.service.workflow;

import ai.lzy.longrunning.OperationRunnerBase.StepResult;
import ai.lzy.util.auth.exceptions.AuthException;
import ai.lzy.util.auth.exceptions.AuthUnavailableException;
import io.grpc.StatusRuntimeException;

import java.sql.SQLException;

import static ai.lzy.util.grpc.GrpcUtils.retryableStatusCode;

public interface RetryableFailStep extends FailContextAwareStep {
    default StepResult retryableFail(Exception exception, String logErrorMes, Runnable freeResources,
                                     StatusRuntimeException defaultSre)
    {
        var retryableError = exception instanceof StatusRuntimeException sre && retryableStatusCode(sre.getStatus()) ||
            exception instanceof SQLException || exception instanceof AuthUnavailableException;
        log().error("{} {}: {}.{}", logPrefix(), logErrorMes, exception.getMessage(),
            (retryableError ? " Reschedule..." : ""), exception);

        if (retryableError) {
            return StepResult.RESTART;
        }

        freeResources.run();

        var exc = (exception instanceof StatusRuntimeException sre) ? sre :
            (exception instanceof AuthException ae) ? ae.toStatusRuntimeException() : defaultSre;

        return failAction().apply(exc);
    }
}
