package ai.lzy.scheduler.jobs;

import ai.lzy.jobsutils.providers.JobProviderBase;
import ai.lzy.model.ReturnCodes;
import ai.lzy.model.db.DbHelper;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.scheduler.db.TaskDao;
import ai.lzy.scheduler.db.impl.SchedulerDataSource;
import ai.lzy.scheduler.models.TaskState;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;
import javax.annotation.Nullable;

public abstract class SchedulerJob extends JobProviderBase<SchedulerJob.Arg> {
    protected final TaskDao dao;
    protected final Logger logger;
    private final SchedulerDataSource storage;

    /**
     * Fail on internal error
     */
    protected void onFail(String id, String execId, Exception e) {
        logger.error("Error while executing job {} for task {} in execution {}: ",
            this.getClass().getName(), id, execId, e);
        try {
            DbHelper.withRetries(logger, () -> failInternal(id, execId, null));
        } catch (Exception ex) {
            logger.error("Error while failing task {}", id, ex);
        }
    }

    protected void fail(String id, String execId, Integer rc, String description,
                        @Nullable TransactionHandle tx) throws SQLException
    {
        logger.error("Task {} failing with rc {} and description <{}>", id, rc, description);
        dao.fail(id, execId, rc, description, tx);
    }

    protected void failInternal(String id, String execId, @Nullable TransactionHandle tx) throws SQLException {
        fail(id, execId, ReturnCodes.INTERNAL_ERROR.getRc(),
            "Some internal error happened while executing task. Please ask for help for lzy team." +
            " Tell them this task id: " + id, tx);
    }

    protected abstract void execute(TaskState task, TransactionHandle tx) throws SQLException;

    @Override
    protected void execute(Arg arg) {
        // Job must be idempotent

        try {
            DbHelper.withRetries(logger, () -> {
                try (TransactionHandle tx = TransactionHandle.create(storage)) {
                    var task = dao.get(arg.taskId, arg.executionId, tx);

                    if (task == null) {
                        logger.error("Cannot get task {} from dao", arg.taskId);
                        return;
                    }

                    execute(task, tx);
                    tx.commit();
                }
            });
        } catch (Exception e) {
            onFail(arg.taskId, arg.executionId, e);
        }
    }

    protected SchedulerJob(TaskDao dao, SchedulerDataSource storage) {
        super(serializer, jobService, Arg.class);
        this.dao = dao;
        this.storage = storage;
        this.logger = LogManager.getLogger(this.getClass());
    }

    public record Arg(
            String taskId,
            String executionId
    ) {}
}
