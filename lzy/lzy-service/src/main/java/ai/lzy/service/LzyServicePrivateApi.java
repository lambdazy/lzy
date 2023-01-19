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
    public void stopExecution(LWFPS.StopExecutionRequest request,
                              StreamObserver<LWFPS.StopExecutionResponse> response)
    {
        var userId = AuthenticationContext.currentSubject().id();
        var executionId = request.getExecutionId();
        var reason = request.getReason();

        if (StringUtils.isEmpty(executionId)) {
            LOG.error("Cannot stop execution: { executionId: {} }", executionId);
            response.onError(Status.INVALID_ARGUMENT.withDescription("Empty 'executionId'").asRuntimeException());
            return;
        }

        LOG.info("Attempt to stop execution: { userId: {}, executionId: {} }", userId, executionId);

        var stopStatus = Status.INTERNAL.withDescription(reason);
        if (cleanExecutionCompanion.markExecutionAsBroken(userId, /* workflowName */ null, executionId, stopStatus)) {
            cleanExecutionCompanion.cleanExecution(executionId);
        }

        response.onNext(LWFPS.StopExecutionResponse.getDefaultInstance());
        response.onCompleted();
    }
}
