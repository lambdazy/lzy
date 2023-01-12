package ai.lzy.scheduler.jobs;

import ai.lzy.jobsutils.JobService;
import ai.lzy.jobsutils.providers.WaitForOperation;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.scheduler.allocator.WorkersAllocator;
import ai.lzy.scheduler.db.TaskDao;
import ai.lzy.scheduler.db.impl.SchedulerDataSource;
import ai.lzy.scheduler.models.TaskState;
import ai.lzy.util.grpc.JsonUtils;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.worker.LWS;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Status;
import jakarta.inject.Singleton;

import java.sql.SQLException;
import javax.annotation.Nullable;

@Singleton
public class AfterExecutionCompleted extends SchedulerOperationConsumer {

    final WorkersAllocator allocator;

    protected AfterExecutionCompleted(JobService jobService, WaitForOperation opProvider,
                                      TaskDao dao, SchedulerDataSource storage, WorkersAllocator allocator)
    {
        super(jobService, opProvider, dao, storage);
        this.allocator = allocator;
    }

    @Override
    protected void execute(String operationId, @Nullable Status.Code code, @Nullable LongRunning.Operation op,
                           TaskState task, @Nullable TransactionHandle tx) throws SQLException
    {
        if (code != null && !code.equals(Status.Code.OK)) {
            logger.error("Error while polling execute operation {}, code: {}, task: {}",
                operationId, code, task.id());

            failInternal(task.id(), task.executionId(), tx);
            return;
        }

        if (op == null) {
            logger.error("Operation is null, but no error code for operation {} for task {}", operationId, task.id());

            failInternal(task.id(), task.executionId(), tx);
            return;
        }

        if (!task.status().equals(TaskState.Status.EXECUTING)) {
            logger.error("Status of task {} is {}, but expected EXECUTING", task.id(), task.status());

            failInternal(task.id(), task.executionId(), tx);
            return;
        }

        if (op.hasError()) {
            logger.error("Error while executing task {}: {}", task.id(), op.getError());

            failInternal(task.id(), task.executionId(), tx);
            return;
        }

        final LWS.ExecuteResponse resp;

        try {
            resp = op.getResponse().unpack(LWS.ExecuteResponse.class);
        } catch (InvalidProtocolBufferException e) {
            logger.error("Error while executing task {}: Cannot unpack resp {} as ExecuteResponse", task.id(),
                JsonUtils.printRequest(op.getResponse()));

            failInternal(task.id(), task.executionId(), tx);
            return;
        }

        allocator.free(task.vmId());

        if (resp.getRc() == 0) {
            dao.updateExecutionCompleted(task.id(), task.executionId(), 0, resp.getDescription(), tx);
        } else {
            dao.fail(task.id(), task.executionId(), resp.getRc(), resp.getDescription(), tx);
        }
    }
}
