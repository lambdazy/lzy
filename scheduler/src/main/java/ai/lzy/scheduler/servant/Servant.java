package ai.lzy.scheduler.servant;

import ai.lzy.model.graph.Env;
import ai.lzy.model.graph.Provisioning;
import ai.lzy.scheduler.models.ServantState.Status;
import ai.lzy.scheduler.task.Task;
import org.apache.curator.shaded.com.google.common.net.HostAndPort;

import java.net.URL;
import javax.annotation.Nullable;

public interface Servant {

    // ========= Events ===========
    void allocate();
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
    String workflowId();
    Provisioning provisioning();
    Status status();

    @Nullable String taskId();
    @Nullable String errorDescription();
    @Nullable HostAndPort servantURL();
}
