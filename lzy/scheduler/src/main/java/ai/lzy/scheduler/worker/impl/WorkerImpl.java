package ai.lzy.scheduler.worker.impl;

import ai.lzy.model.operation.Operation;
import ai.lzy.scheduler.models.WorkerEvent;
import ai.lzy.scheduler.models.WorkerState;
import ai.lzy.scheduler.task.Task;
import ai.lzy.scheduler.worker.Worker;
import com.google.common.net.HostAndPort;
import org.jetbrains.annotations.Nullable;

public class WorkerImpl implements Worker {
    private final WorkerState state;
    private final EventQueue events;

    public WorkerImpl(WorkerState state, EventQueue events) {
        this.state = state;
        this.events = events;
    }

    @Override
    public void notifyConnected(HostAndPort workerUrl) {
        events.put(WorkerEvent.fromState(state, WorkerEvent.Type.CONNECTED)
            .setWorkerUrl(workerUrl)
            .setDescription("Worker connected to scheduler")
            .build());
    }

    @Override
    public void notifyConfigured(int rc, String description) {
        events.put(WorkerEvent.fromState(state, WorkerEvent.Type.CONFIGURED)
            .setRc(rc)
            .setDescription(description)
            .build());
    }

    @Override
    public void setTask(Task task) {
        events.put(WorkerEvent.fromState(state, WorkerEvent.Type.EXECUTION_REQUESTED)
            .setTaskId(task.taskId())
            .setDescription("Execution of task <" + task.taskId() + "> requested")
            .build());
    }

    @Override
    public void notifyExecutionCompleted(int rc, String description) {
        events.put(WorkerEvent.fromState(state, WorkerEvent.Type.EXECUTION_COMPLETED)
            .setDescription(description)
            .setRc(rc)
            .build());
    }

    @Override
    public void notifyCommunicationCompleted() {
        events.put(WorkerEvent.fromState(state, WorkerEvent.Type.COMMUNICATION_COMPLETED)
            .setDescription("All slots of worker closed")
            .build());
    }

    @Override
    public void stop(String issue) {
        events.put(WorkerEvent.fromState(state, WorkerEvent.Type.STOP)
            .setDescription(issue)
            .build());
    }

    @Override
    public void notifyStopped(int rc, String description) {
        events.put(WorkerEvent.fromState(state, WorkerEvent.Type.STOPPED)
            .setDescription(description)
            .setRc(rc)
            .build());
    }

    @Override
    public void executingHeartbeat() {
        events.put(WorkerEvent.fromState(state, WorkerEvent.Type.EXECUTING_HEARTBEAT)
            .setDescription("Executing heartbeat")
            .build());
    }

    @Override
    public void idleHeartbeat() {
        events.put(WorkerEvent.fromState(state, WorkerEvent.Type.IDLE_HEARTBEAT)
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
    public WorkerState.Status status() {
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
    public HostAndPort workerURL() {
        return state.workerUrl();
    }
}
