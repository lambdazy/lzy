package ai.lzy.service.operations;

import ai.lzy.longrunning.OperationRunnerBase.StepResult;
import ai.lzy.util.auth.exceptions.AuthException;
import ai.lzy.util.auth.exceptions.AuthUnavailableException;
import io.grpc.StatusRuntimeException;

import java.sql.SQLException;

import static ai.lzy.util.grpc.GrpcUtils.retryableStatusCode;

public interface RetryableFailStep extends FailContextAwareStep {
    default StepResult retryableFail(Exception e, String logErrorMes, Runnable free,
                                     StatusRuntimeException defaultSre)
    {
        var retryableError = e instanceof StatusRuntimeException sre && retryableStatusCode(sre.getStatus()) ||
            e instanceof SQLException || e instanceof AuthUnavailableException;
        log().error("{} {}: {}.{}", logPrefix(), logErrorMes, e.getMessage(), (retryableError ? " Reschedule..." : ""),
            e);

        if (retryableError) {
            return StepResult.RESTART;
        }

        free.run();

        var exc = (e instanceof StatusRuntimeException sre) ? sre :
            (e instanceof AuthException ae) ? ae.toStatusRuntimeException() : defaultSre;

        return failAction().apply(exc);
    }

    default StepResult retryableFail(Exception e, String logErrorMes, StatusRuntimeException defaultSre) {
        return retryableFail(e, logErrorMes, () -> {}, defaultSre);
    }
}
