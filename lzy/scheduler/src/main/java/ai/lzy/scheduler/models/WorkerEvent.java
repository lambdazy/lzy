package ai.lzy.scheduler.models;

import com.google.common.net.HostAndPort;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

public record WorkerEvent(
    String id,
    Instant timestamp,
    String workerId,
    String workflowName,
    Type type,

    @Nullable String description,
    @Nullable Integer rc,
    @Nullable String taskId,
    @Nullable HostAndPort workerUrl
) implements Delayed {

    public static EventBuilder fromState(WorkerState state, Type type) {
        return new EventBuilder(state, type);
    }

    public static WorkerEvent noop(String workflowName, String workerId) {
        return new WorkerEvent(UUID.randomUUID().toString(),
            Instant.now(), workerId, workflowName, Type.NOOP, "NOOP", null, null, null);
    }

    @Override
    public int compareTo(@NotNull Delayed o) {
        return Long.compare(this.getDelay(TimeUnit.NANOSECONDS), o.getDelay(TimeUnit.NANOSECONDS));
    }

    @Override
    public long getDelay(@NotNull TimeUnit unit) {
        return unit.toChronoUnit().between(Instant.now(), timestamp);
    }

    public enum Type {
        NOOP,  // empty event to interrupt queue. Does not save to db
        ALLOCATION_TIMEOUT,
        CONNECTED,  // Worker connected to scheduler with method register
        CONFIGURED,  // Worker env configuration completed
        CONFIGURATION_TIMEOUT,  // Worker configuration timed out
        EXECUTION_REQUESTED,  // Task requests execution
        EXECUTING_HEARTBEAT,  // Heartbeat of worker while its executing task
        EXECUTING_HEARTBEAT_TIMEOUT,  // Timeout of heartbeat waiting of worker
        EXECUTION_COMPLETED,  // Worker says that execution was completed
        EXECUTION_TIMEOUT,  // Executing longer than execution timeout
        COMMUNICATION_COMPLETED,  // All data from worker was uploaded
        IDLE_HEARTBEAT,  // Heartbeat of worker while its idle
        IDLE_HEARTBEAT_TIMEOUT,  // Timeout of heartbeat waiting of worker
        IDLE_TIMEOUT,  // Worker is too long unused
        STOP,  // Stop requested
        STOPPING_TIMEOUT,  // Timeout of graceful shutdown of worker
        STOPPED  // Worker exited
    }

    @Override
    public String toString() {
        return String.format("<type:%s, timestamp: %s>", type, timestamp);
    }

    public static class EventBuilder {
        private final WorkerState state;
        private final Type type;
        private Instant timestamp = Instant.now();
        private Integer rc = null;
        private String description = null;
        private String taskId = null;
        private HostAndPort workerUrl = null;

        private EventBuilder(WorkerState state, Type type) {
            this.state = state;
            this.type = type;
        }

        public EventBuilder setRc(Integer rc) {
            this.rc = rc;
            return this;
        }

        public EventBuilder setDescription(String description) {
            this.description = description;
            return this;
        }

        public EventBuilder setTimeout(Duration timeout) {
            this.timestamp = Instant.now().plus(timeout);
            return this;
        }

        public EventBuilder setTaskId(String taskId) {
            this.taskId = taskId;
            return this;
        }

        public EventBuilder setWorkerUrl(HostAndPort workerUrl) {
            this.workerUrl = workerUrl;
            return this;
        }

        public WorkerEvent build() {
            return new WorkerEvent(
                UUID.randomUUID().toString(),
                timestamp,
                state.id(),
                state.workflowName(),
                type,
                description,
                rc,
                taskId,
                workerUrl
            );
        }
    }
}
