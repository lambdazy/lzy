package ai.lzy.scheduler.servant.impl;

import ai.lzy.model.graph.Env;
import ai.lzy.model.graph.Provisioning;
import ai.lzy.scheduler.models.ServantEvent;
import ai.lzy.scheduler.models.ServantState;
import ai.lzy.scheduler.servant.Servant;
import ai.lzy.scheduler.task.Task;
import java.net.URL;
import org.jetbrains.annotations.Nullable;

public class ServantImpl implements Servant {
    private final ServantState state;
    private final EventQueue events;

    public ServantImpl(ServantState state, EventQueue events) {
        this.state = state;
        this.events = events;
    }

    @Override
    public void allocate() {
        events.put(ServantEvent.fromState(state, ServantEvent.Type.ALLOCATION_REQUESTED)
            .setDescription("Allocation of servant requested")
            .build());
    }

    @Override
    public void notifyConnected(URL servantUrl) {
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
    public void notifyDisconnected() {
        events.put(ServantEvent.fromState(state, ServantEvent.Type.DISCONNECTED)
            .setDescription("Servant disconnected")
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
    public void signal(int signalNum) {
        events.put(ServantEvent.fromState(state, ServantEvent.Type.SIGNAL)
            .setDescription("Signal sent")
            .setSignalNumber(signalNum)
            .build());
    }

    @Override
    public String id() {
        return state.id();
    }

    @Override
    public String workflowId() {
        return state.workflowId();
    }

    @Override
    public Provisioning provisioning() {
        return state.provisioning();
    }

    @Override
    public ServantState.Status status() {
        return state.status();
    }

    @Override
    public Env env() {
        return state.env();
    }

    @Override
    public String allocatorMetadata() {
        return state.allocatorMeta();
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
    public String allocationToken() {
        return state.allocationToken();
    }

    @Nullable
    @Override
    public URL servantURL() {
        return state.servantUrl();
    }
}
