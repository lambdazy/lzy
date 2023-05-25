package ai.lzy.service.operations.start;

import ai.lzy.iam.grpc.client.AccessBindingServiceGrpcClient;
import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.iam.resources.AccessBinding;
import ai.lzy.iam.resources.Role;
import ai.lzy.iam.resources.credentials.SubjectCredentials;
import ai.lzy.iam.resources.impl.Workflow;
import ai.lzy.iam.resources.subjects.AuthProvider;
import ai.lzy.iam.resources.subjects.CredentialsType;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.longrunning.OperationRunnerBase.StepResult;
import ai.lzy.service.config.PortalServiceSpec;
import ai.lzy.service.dao.StartExecutionState;
import ai.lzy.service.operations.ExecutionStepContext;
import ai.lzy.service.operations.RetryableFailStep;
import ai.lzy.util.auth.exceptions.AuthException;
import io.grpc.Status;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import static ai.lzy.model.db.DbHelper.withRetries;

final class CreatePortalSubject extends StartExecutionContextAwareStep
    implements Supplier<StepResult>, RetryableFailStep
{
    private final PortalServiceSpec spec;
    private final SubjectServiceGrpcClient subjClient;
    private final AccessBindingServiceGrpcClient abClient;

    public CreatePortalSubject(ExecutionStepContext stepCtx, StartExecutionState state, PortalServiceSpec spec,
                               SubjectServiceGrpcClient subjClient, AccessBindingServiceGrpcClient abClient)
    {
        super(stepCtx, state);
        this.spec = spec;
        this.subjClient = subjClient;
        this.abClient = abClient;
    }

    @Override
    public StepResult get() {
        if (portalSubjectId() != null) {
            log().debug("{} Portal iam subject already created, skip step...", logPrefix());
            return StepResult.ALREADY_DONE;
        }

        if (portalId() == null) {
            setPortalId(idGenerator().generate());

            try {
                withRetries(log(), () -> execDao().updatePortalId(execId(), portalId(), null));
            } catch (Exception e) {
                return retryableFail(e, "Cannot save portal id", () -> {}, Status.INTERNAL.withDescription(
                    "Cannot create portal subject").asRuntimeException());
            }
        }

        log().info("{} Creating iam subject for portal of execution: { wfName: {}, execId: {}, portalId: {} }",
            logPrefix(), wfName(), execId(), portalId());

        final Subject subject;

        try {
            subject = subjClient.createSubject(AuthProvider.INTERNAL, portalId(), SubjectType.WORKER,
                new SubjectCredentials("main", spec.rsaKeys().publicKey(), CredentialsType.PUBLIC_KEY));
        } catch (Exception e) {
            return retryableFail(e, "Error in SubjectClient::create call for portal", () -> {}, Status.INTERNAL
                .withDescription("Cannot create portal subject").asRuntimeException());
        }

        Function<Exception, Runnable> removeSubject = e -> () -> {
            try {
                subjClient.removeSubject(subject);
            } catch (AuthException sre) {
                log().warn("{} Cannot remove portal iam subject with id='{}' after error {}: ", logPrefix(),
                    subject.id(), e.getMessage(), sre);
            }
        };

        try {
            var wfResource = new Workflow(userId() + "/" + wfName());
            var bindings = List.of(new AccessBinding(Role.LZY_WORKFLOW_OWNER, subject));
            abClient.setAccessBindings(wfResource, bindings);
        } catch (Exception e) {
            return retryableFail(e, "Error in AccessBinding:set call for iam subject with id='%s'".formatted(
                subject.id()), removeSubject.apply(e), Status.INTERNAL.withDescription("Cannot create portal subject")
                .asRuntimeException());
        }

        try {
            withRetries(log(), () -> execDao().updatePortalSubjectId(execId(), subject.id(), null));
        } catch (Exception e) {
            return retryableFail(e, "Cannot save portal iam subject id", removeSubject.apply(e), Status.INTERNAL
                .withDescription("Cannot create portal subject").asRuntimeException());
        }

        setPortalSubjectId(subject.id());
        return StepResult.CONTINUE;
    }
}
