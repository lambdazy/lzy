package ai.lzy.scheduler.servant;

import ai.lzy.model.operation.Operation;
import ai.lzy.scheduler.models.ServantState.Status;
import ai.lzy.scheduler.task.Task;
import org.apache.curator.shaded.com.google.common.net.HostAndPort;

import javax.annotation.Nullable;

public interface Servant {

    // ========= Events ===========
    void notifyConnected(HostAndPort servantUrl);
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
    String workflowName();
    Operation.Requirements requirements();
    Status status();

    @Nullable String taskId();
    @Nullable String errorDescription();
    @Nullable HostAndPort servantURL();
}
