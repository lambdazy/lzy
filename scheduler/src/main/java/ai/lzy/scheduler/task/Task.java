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
    TaskDesc description();
    TaskState.Status status();

    @Nullable String servantId();
    @Nullable Integer rc();
    @Nullable String errorDescription();

    // ========= Servant events ==========
    void notifyScheduled() throws DaoException;
    void notifyExecuting(String servantId) throws DaoException;
    void notifyExecutionCompleted(Integer rc, String description) throws DaoException;
}
