package ai.lzy.longrunning.task;

import ai.lzy.longrunning.OperationRunnerBase;
import ai.lzy.longrunning.OperationsExecutor;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.longrunning.task.dao.OperationTaskDao;
import ai.lzy.model.db.Storage;

import java.time.Duration;
import java.util.Map;

import static ai.lzy.model.db.DbHelper.withRetries;

public abstract class OpTaskAwareAction extends OperationRunnerBase {
    private final OperationTaskExecutor operationTaskExecutor;
    private final OperationTaskDao operationTaskDao;
    private final Duration leaseDuration;
    private OperationTask operationTask;

    public OpTaskAwareAction(OperationTask operationTask, OperationTaskDao operationTaskDao, Duration leaseDuration,
                             String opId, String desc, Storage storage, OperationDao operationsDao,
                             OperationsExecutor executor, OperationTaskExecutor operationTaskExecutor)
    {
        super(opId, desc, storage, operationsDao, executor);
        this.operationTask = operationTask;
        this.operationTaskDao = operationTaskDao;
        this.leaseDuration = leaseDuration;
        this.operationTaskExecutor = operationTaskExecutor;
    }

    @Override
    protected Map<String, String> prepareLogContext() {
        var ctx = super.prepareLogContext();
        ctx.put("task_id", String.valueOf(operationTask.id()));
        ctx.put("task_type", operationTask.type());
        ctx.put("task_name", operationTask.name());
        ctx.put("task_entity_id", operationTask.entityId());
        return ctx;
    }

    protected OperationTask task() {
        return operationTask;
    }

    public void setTask(OperationTask operationTask) {
        this.operationTask = operationTask;
    }

    @Override
    protected void beforeStep() {
        super.beforeStep();
        try {
            operationTask = withRetries(log(), () -> operationTaskDao.updateLease(operationTask.id(), leaseDuration,
                null));
        } catch (Exception e) {
            log().error("{} Couldn't update lease on task {}", logPrefix(), task().id());
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void notifyFinished() {
        var builder = OperationTask.Update.builder();
        if (isFailed()) {
            builder.status(OperationTask.Status.FAILED);
        } else {
            builder.status(OperationTask.Status.FINISHED);
        }
        try {
            operationTask = withRetries(log(), () -> operationTaskDao.update(operationTask.id(), builder.build(),
                null));
        } catch (Exception e) {
            log().error("{} Couldn't finish operation task {}", logPrefix(), task().id());
        }
        operationTaskExecutor.releaseTask(task());
    }
}
