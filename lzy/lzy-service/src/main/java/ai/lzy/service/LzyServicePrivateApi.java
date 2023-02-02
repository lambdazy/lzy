package ai.lzy.service;

import ai.lzy.iam.grpc.context.AuthenticationContext;
import ai.lzy.v1.workflow.LWFPS;
import ai.lzy.v1.workflow.LzyWorkflowPrivateServiceGrpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.micronaut.core.util.StringUtils;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@Singleton
public class LzyServicePrivateApi extends LzyWorkflowPrivateServiceGrpc.LzyWorkflowPrivateServiceImplBase {
    private static final Logger LOG = LogManager.getLogger(LzyServicePrivateApi.class);

    private final CleanExecutionCompanion cleanExecutionCompanion;

    public LzyServicePrivateApi(CleanExecutionCompanion cleanExecutionCompanion) {
        this.cleanExecutionCompanion = cleanExecutionCompanion;
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

        var abortStatus = Status.INTERNAL.withDescription(reason);
        try {
            cleanExecutionCompanion.markExecutionAsBroken(userId, /* workflowName */ null, executionId, abortStatus);
            cleanExecutionCompanion.cleanExecution(executionId);
        } catch (Exception e) {
            LOG.error("Cannot abort execution: { executionId: {} }", executionId, e);
            response.onError(Status.INTERNAL.withDescription("Cannot abort execution").asRuntimeException());
            return;
        }

        response.onNext(LWFPS.AbortExecutionResponse.getDefaultInstance());
        response.onCompleted();
    }
}
