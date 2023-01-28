package ai.lzy.scheduler.task;

import ai.lzy.model.TaskDesc;
import ai.lzy.model.db.exceptions.DaoException;
import ai.lzy.scheduler.db.TaskDao;
import ai.lzy.scheduler.models.TaskState;
import ai.lzy.scheduler.models.TaskState.Status;
import org.jetbrains.annotations.Nullable;

public class TaskImpl implements Task {

    private TaskState state;
    private final TaskDao dao;

    public TaskImpl(TaskState state, TaskDao dao) {
        this.state = state;
        this.dao = dao;
    }

    @Override
    public String taskId() {
        return state.id();
    }

    @Override
    public String workflowId() {
        return state.workflowId();
    }

    @Override
    public String workflowName() {
        return state.workflowName();
    }

    @Override
    public String userId() {
        return state.userId();
    }

    @Override
    public TaskDesc description() {
        return state.description();
    }

    @Override
    public Status status() {
        return state.status();
    }

    @Nullable
    @Override
    public String workerId() {
        return state.workerId();
    }

    @Nullable
    @Override
    public Integer rc() {
        return state.returnCode();
    }

    @Nullable
    @Override
    public String errorDescription() {
        return state.errorDescription();
    }

    @Override
    public void notifyScheduled() throws DaoException {
        state = new TaskState(taskId(), workflowId(), workflowName(), userId(), description(),
                Status.SCHEDULED, rc(), errorDescription(), workerId());
        dao.update(this);
    }

    @Override
    public void notifyExecuting(String workerId) throws DaoException {
        state = new TaskState(taskId(), workflowId(), workflowName(), userId(), description(),
            Status.EXECUTING, rc(), errorDescription(), workerId);
        dao.update(this);
    }

    @Override
    public void notifyExecutionCompleted(Integer rc, String description) throws DaoException {
        final Status status = rc == 0 ? Status.SUCCESS : Status.ERROR;
        state = new TaskState(taskId(), workflowId(), workflowName(), userId(), description(),
            status, rc, description, workerId());
        dao.update(this);
    }
}