package ai.lzy.service.workflow.start;

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
import ai.lzy.service.data.dao.ExecutionDao;
import ai.lzy.util.auth.credentials.RsaUtils;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static ai.lzy.model.db.DbHelper.withRetries;

public class CreatePortalSubject implements Supplier<StepResult> {
    private final ExecutionDao execDao;
    private final SubjectServiceGrpcClient subjClient;
    private final AccessBindingServiceGrpcClient abClient;
    private final String userId;
    private final String execId;
    private final String wfName;
    private final String portalId;
    private final Consumer<String> portalIdConsumer;
    private final Consumer<String> subjectIdConsumer;
    private final Function<StatusRuntimeException, StepResult> failAction;
    private final Logger log;
    private final String logPrefix;

    public CreatePortalSubject(ExecutionDao execDao, String userId, String execId, String wfName,
                               SubjectServiceGrpcClient subjClient, AccessBindingServiceGrpcClient abClient,
                               Consumer<String> portalIdConsumer, Consumer<String> subjectIdConsumer,
                               Function<StatusRuntimeException, StepResult> failAction, Logger log, String logPrefix)
    {
        this.execDao = execDao;
        this.subjClient = subjClient;
        this.abClient = abClient;
        this.userId = userId;
        this.execId = execId;
        this.wfName = wfName;
        this.portalId = "portal_" + execId;
        this.portalIdConsumer = portalIdConsumer;
        this.subjectIdConsumer = subjectIdConsumer;
        this.failAction = failAction;
        this.log = log;
        this.logPrefix = logPrefix;
    }

    @Override
    public StepResult get() {
        log.info("{} Creating iam subject for portal of execution: { wfName: {}, execId: {}, portalId: {} }",
            logPrefix, wfName, execId, portalId);

        final Subject subject;

        try {
            var workerKeys = RsaUtils.generateRsaKeys();
            subject = subjClient.createSubject(AuthProvider.INTERNAL, portalId, SubjectType.WORKER,
                new SubjectCredentials("main", workerKeys.publicKey(), CredentialsType.PUBLIC_KEY));
        } catch (StatusRuntimeException sre) {
            if (sre.getStatus().getCode() != Status.Code.ALREADY_EXISTS) {
                log.error("{} Error in SubjectService::create call for portal: {}", logPrefix, sre.getMessage(), sre);
                return failAction.apply(sre);
            }
            log.warn("{} Subject already exists: {}", logPrefix, sre.getMessage());
            return StepResult.ALREADY_DONE;
        } catch (Exception e) {
            log.error("{} Error in SubjectClient::create call for portal: {}", logPrefix, e.getMessage(), e);
            return failAction.apply(Status.INTERNAL.withDescription("Cannot create portal subject")
                .asRuntimeException());
        }

        try {
            var wfResource = new Workflow(userId + "/" + wfName);
            var bindings = List.of(new AccessBinding(Role.LZY_WORKFLOW_OWNER, subject));
            abClient.setAccessBindings(wfResource, bindings);
        } catch (Exception e) {
            log.error("{} Error in AccessBinding:set call for portal iam subject with id='{}': {}", logPrefix,
                subject.id(), e.getMessage(), e);
            try {
                subjClient.removeSubject(subject);
            } catch (StatusRuntimeException sre) {
                log.warn("{} Cannot remove portal iam subject with id='{}' after error {}: ", logPrefix, subject.id(),
                    e.getMessage(), sre);
            }
            return failAction.apply(Status.INTERNAL.withDescription("Cannot create portal subject")
                .asRuntimeException());
        }

        portalIdConsumer.accept(portalId);
        subjectIdConsumer.accept(subject.id());

        try {
            withRetries(log, () -> execDao.updatePortalSubjectId(execId, portalId, subject.id(), null));
        } catch (Exception e) {
            log.error("{} Cannot save data about portal iam subject with id='{}': {}", logPrefix, subject.id(),
                e.getMessage(), e);
            try {
                subjClient.removeSubject(subject);
            } catch (StatusRuntimeException sre) {
                log.warn("{} Cannot remove portal iam subject with id='{}' after error {}: ", logPrefix, subject.id(),
                    e.getMessage(), sre);
            }
            return failAction.apply(Status.INTERNAL.withDescription("Cannot create portal subject")
                .asRuntimeException());
        }

        return StepResult.CONTINUE;
    }
}
