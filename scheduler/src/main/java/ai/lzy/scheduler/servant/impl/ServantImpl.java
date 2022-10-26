package ai.lzy.scheduler.servant.impl;

import ai.lzy.model.operation.Operation;
import ai.lzy.scheduler.models.ServantEvent;
import ai.lzy.scheduler.models.ServantState;
import ai.lzy.scheduler.servant.Servant;
import ai.lzy.scheduler.task.Task;
import com.google.common.net.HostAndPort;
import org.jetbrains.annotations.Nullable;

public class ServantImpl implements Servant {
    private final ServantState state;
    private final EventQueue events;

    public ServantImpl(ServantState state, EventQueue events) {
        this.state = state;
        this.events = events;
    }

    @Override
    public void notifyConnected(HostAndPort servantUrl) {
        events.put(ServantEvent.fromState(state, ServantEvent.Type.CONNECTED)
            .setServantUrl(servantUrl)
            .setDescription("Servant connected to scheduler")
            .build());
    }

    @Override
    public void notifyConfigured(int rc, String description) {
        events.put(ServantEvent.fromState(state, ServantEvent.Type.CONFIGURED)
            .setRc(rc)
            .setDescription(description)
            .build());
    }

    @Override
    public void setTask(Task task) {
        events.put(ServantEvent.fromState(state, ServantEvent.Type.EXECUTION_REQUESTED)
            .setTaskId(task.taskId())
            .setDescription("Execution of task <" + task.taskId() + "> requested")
            .build());
    }

    @Override
    public void notifyExecutionCompleted(int rc, String description) {
        events.put(ServantEvent.fromState(state, ServantEvent.Type.EXECUTION_COMPLETED)
            .setDescription(description)
            .setRc(rc)
            .build());
    }

    @Override
    public void notifyCommunicationCompleted() {
        events.put(ServantEvent.fromState(state, ServantEvent.Type.COMMUNICATION_COMPLETED)
            .setDescription("All slots of servant closed")
            .build());
    }

    @Override
    public void stop(String issue) {
        events.put(ServantEvent.fromState(state, ServantEvent.Type.STOP)
            .setDescription(issue)
            .build());
    }

    @Override
    public void notifyStopped(int rc, String description) {
        events.put(ServantEvent.fromState(state, ServantEvent.Type.STOPPED)
            .setDescription(description)
            .setRc(rc)
            .build());
    }

    @Override
    public void executingHeartbeat() {
        events.put(ServantEvent.fromState(state, ServantEvent.Type.EXECUTING_HEARTBEAT)
            .setDescription("Executing heartbeat")
            .build());
    }

    @Override
    public void idleHeartbeat() {
        events.put(ServantEvent.fromState(state, ServantEvent.Type.IDLE_HEARTBEAT)
            .setDescription("Idle heartbeat")
            .build());
    }

    @Override
    public String id() {
        return state.id();
    }

    @Override
    public String userId() {
        return state.userId();
    }

    @Override
    public String workflowName() {
        return state.workflowName();
    }

    @Override
    public Operation.Requirements requirements() {
        return state.requirements();
    }

    @Override
    public ServantState.Status status() {
        return state.status();
    }

    @Nullable
    @Override
    public String taskId() {
        return state.taskId();
    }

    @Nullable
    @Override
    public String errorDescription() {
        return state.errorDescription();
    }

    @Nullable
    @Override
    public HostAndPort servantURL() {
        return state.servantUrl();
    }
}
