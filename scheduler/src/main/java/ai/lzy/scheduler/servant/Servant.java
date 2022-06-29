package ai.lzy.scheduler.servant;

import ai.lzy.model.graph.Env;
import ai.lzy.model.graph.Provisioning;
import ai.lzy.scheduler.models.ServantState.Status;
import ai.lzy.scheduler.task.Task;
import java.net.URL;
import javax.annotation.Nullable;

public interface Servant {

    // ========= Events ===========
    void allocate();
    void notifyConnected(URL servantUrl);
    void notifyConfigured(int rc, String description);
    void notifyDisconnected();
    void setTask(Task task);
    void notifyExecutionCompleted(int rc, String description);
    void notifyCommunicationCompleted();
    void stop(String issue);
    void notifyStopped(int rc, String description);
    void signal(int signalNum);

    // ========= Fields ===========
    String id();
    String workflowId();
    Provisioning provisioning();
    Status status();
    Env env();

    @Nullable String taskId();
    @Nullable String errorDescription();
    @Nullable String allocationToken();
    @Nullable URL servantURL();
    @Nullable String allocatorMetadata();
}
