package ai.lzy.scheduler.task;

import ai.lzy.model.TaskDesc;
import ai.lzy.model.db.exceptions.DaoException;
import ai.lzy.scheduler.models.TaskState;

import javax.annotation.Nullable;

public interface Task {
    Task NOOP = new TaskImpl(null, null);

    // ========= Fields ==========
    String taskId();
    String workflowId();
    String workflowName();
    String userId();
    TaskDesc description();
    TaskState.Status status();

    @Nullable String workerId();
    @Nullable Integer rc();
    @Nullable String errorDescription();

    // ========= Worker events ==========
    void notifyScheduled() throws DaoException;
    void notifyExecuting(String workerId) throws DaoException;
    void notifyExecutionCompleted(Integer rc, String description) throws DaoException;
}
