package ai.lzy.longrunning.task;

import ai.lzy.longrunning.OperationRunnerBase;
import ai.lzy.longrunning.OperationsExecutor;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.Storage;
import jakarta.annotation.Nullable;

import java.time.Duration;
import java.util.Map;

public abstract class TaskAwareAction extends OperationRunnerBase {
    private final TaskQueue queue;
    private final Duration leaseDuration;
    private Task task;

    public TaskAwareAction(Task task, TaskQueue queue, Duration leaseDuration, String opId, String desc,
                           Storage storage, OperationDao operationsDao, OperationsExecutor executor)
    {
        super(opId, desc, storage, operationsDao, executor);
        this.task = task;
        this.queue = queue;
        this.leaseDuration = leaseDuration;
    }

    @Override
    protected Map<String, String> prepareLogContext() {
        var ctx = super.prepareLogContext();
        ctx.put("task_id", String.valueOf(task.id()));
        ctx.put("task_type", task.type());
        ctx.put("task_name", task.name());
        ctx.put("task_entity_id", task.entityId());
        return ctx;
    }

    protected Task task() {
        return task;
    }

    @Override
    protected void beforeStep() {
        super.beforeStep();
        task = queue.updateLease(task.id(), leaseDuration);
    }

    @Override
    protected void notifyFinished(@Nullable Throwable t) {
        super.notifyFinished(t);

        var builder = Task.Update.builder();
        if (t != null) {
            builder.status(Task.Status.FAILED);
        } else {
            builder.status(Task.Status.FINISHED);
        }
        task = queue.update(task.id(), builder.build());
    }
}
