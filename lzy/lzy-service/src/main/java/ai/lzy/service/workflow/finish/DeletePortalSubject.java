package ai.lzy.service.workflow.finish;

import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.iam.resources.subjects.Worker;
import ai.lzy.longrunning.OperationRunnerBase.StepResult;
import ai.lzy.service.data.dao.ExecutionDao;
import ai.lzy.util.auth.exceptions.AuthException;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.annotation.Nullable;
import org.apache.logging.log4j.Logger;

import java.util.function.Function;
import java.util.function.Supplier;

import static ai.lzy.model.db.DbHelper.withRetries;

public class DeletePortalSubject implements Supplier<StepResult> {
    private final ExecutionDao execDao;
    private final String execId;
    private final String subjectId;
    private final SubjectServiceGrpcClient subjClient;
    private final Function<StatusRuntimeException, StepResult> failAction;
    private final Logger log;
    private final String logPrefix;

    public DeletePortalSubject(ExecutionDao execDao, String execId, @Nullable String subjectId,
                               SubjectServiceGrpcClient subjClient,
                               Function<StatusRuntimeException, StepResult> failAction,
                               Logger log, String logPrefix)
    {
        this.execDao = execDao;
        this.execId = execId;
        this.subjectId = subjectId;
        this.subjClient = subjClient;
        this.failAction = failAction;
        this.log = log;
        this.logPrefix = logPrefix;
    }

    @Override
    public StepResult get() {
        if (subjectId == null) {
            log.debug("{} Portal subject id is null, skip step...", logPrefix);
            return StepResult.ALREADY_DONE;
        }

        log.info("{} Delete portal iam subject: { subjectId: {} }", logPrefix, subjectId);

        try {
            subjClient.removeSubject(new Worker(subjectId));
            withRetries(log, () -> execDao.updatePortalSubjectId(execId, null, null, null));
        } catch (AuthException e) {
            log.warn("{} Error while deleting portal subject: {}", logPrefix, e.getMessage(), e);
            return failAction.apply(Status.INTERNAL.withDescription("Cannot delete portal subject")
                .asRuntimeException());
        } catch (Exception e) {
            log.warn("{} Error while cleaning portal subject id in dao: {}", logPrefix, e.getMessage(), e);
            return failAction.apply(Status.INTERNAL.withDescription("Cannot delete portal subject")
                .asRuntimeException());
        }

        return StepResult.CONTINUE;
    }
}
