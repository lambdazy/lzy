package ai.lzy.scheduler.worker;

import ai.lzy.model.operation.Operation;
import ai.lzy.scheduler.models.WorkerState.Status;
import ai.lzy.scheduler.task.Task;
import com.google.common.net.HostAndPort;

import javax.annotation.Nullable;

public interface Worker {

    // ========= Events ===========
    void notifyConnected(HostAndPort workerUrl);
    void notifyConfigured(int rc, String description);
    void setTask(Task task);
    void notifyExecutionCompleted(int rc, String description);
    void notifyCommunicationCompleted();
    void stop(String issue);
    void notifyStopped(int rc, String description);

    void executingHeartbeat();
    void idleHeartbeat();

    // ========= Fields ===========
    String id();
    String userId();
    String workflowName();
    Operation.Requirements requirements();
    Status status();

    @Nullable String taskId();
    @Nullable String errorDescription();
    @Nullable HostAndPort workerURL();
}
