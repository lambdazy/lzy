package ai.lzy.scheduler.jobs;

import ai.lzy.jobsutils.JobService;
import ai.lzy.jobsutils.providers.WaitForOperation;
import ai.lzy.model.ReturnCodes;
import ai.lzy.model.db.DbHelper;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.scheduler.db.TaskDao;
import ai.lzy.scheduler.db.impl.SchedulerDataSource;
import ai.lzy.scheduler.models.TaskState;
import ai.lzy.v1.longrunning.LongRunning;
import io.grpc.Status;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nullable;
import java.sql.SQLException;

public abstract class SchedulerOperationConsumer extends WaitForOperation.OperationConsumer<SchedulerJob.Arg> {
    protected final TaskDao dao;
    protected final Logger logger;
    private final SchedulerDataSource storage;

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

    protected abstract void execute(String operationId, @Nullable Status.Code code, @Nullable LongRunning.Operation op,
                                    TaskState task, @Nullable TransactionHandle tx) throws SQLException;

    @Override
    protected void execute(String operationId, @Nullable Status.Code code,
                           @Nullable LongRunning.Operation op, SchedulerJob.Arg arg)
    {
        // Job must be idempotent
        try {
            DbHelper.withRetries(logger, () -> {
                try (TransactionHandle tx = TransactionHandle.create(storage)) {
                    var task = dao.get(arg.taskId(), arg.executionId(), tx);

                    if (task == null) {
                        logger.error("Cannot get task {} from dao", arg.taskId());
                        return;
                    }

                    execute(operationId, code, op, task, tx);
                    tx.commit();
                }
            });
        } catch (Exception e) {
            onFail(arg.taskId(), arg.executionId(), e);
        }
    }

    protected SchedulerOperationConsumer(JobService jobService, WaitForOperation opProvider,
                                         TaskDao dao, SchedulerDataSource storage)
    {
        super(jobService, opProvider, SchedulerJob.Arg.class);
        this.dao = dao;
        this.logger = LogManager.getLogger(this.getClass());
        this.storage = storage;
    }
}
