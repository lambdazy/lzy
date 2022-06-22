package ai.lzy.scheduler.task;

import ai.lzy.scheduler.models.TaskDesc;
import ai.lzy.scheduler.models.TaskState;
import javax.annotation.Nullable;

public interface Task {
    // ========= Fields ==========
    String taskId();
    String workflowId();
    TaskDesc description();
    TaskState.Status status();

    @Nullable String servantId();
    @Nullable Integer rc();
    @Nullable String errorDescription();

    // ========= Servant events ==========
    void notifyExecuting(String servantId);
    void notifyExecutionCompleted(Integer rc, String description);
}
