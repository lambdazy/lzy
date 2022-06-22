package ai.lzy.scheduler.models;

import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.jetbrains.annotations.NotNull;

public record ServantEvent(
    String id,
    LocalDateTime timestamp,
    String servantId,
    String workflowId,
    Type type,

    @Nullable String description,
    @Nullable Integer rc,
    @Nullable String taskId,
    @Nullable Integer signalNumber
) implements Delayed {

    public static EventBuilder fromState(ServantState state, Type type) {
        return new EventBuilder(state, type);
    }

    @Override
    public int compareTo(@NotNull Delayed o) {
        return Long.compare(this.getDelay(TimeUnit.NANOSECONDS), o.getDelay(TimeUnit.NANOSECONDS));
    }

    @Override
    public long getDelay(@NotNull TimeUnit unit) {
        return unit.toChronoUnit().between(LocalDateTime.now(), timestamp);
    }

    public enum Type {
        ALLOCATION_REQUESTED,  // Event that represents allocation start
        ALLOCATION_TIMEOUT,
        CONNECTED,  // Servant connected to scheduler with method register
        CONFIGURED,  // Servant env configuration completed
        CONFIGURATION_TIMEOUT,  // Servant configuration timed out
        EXECUTION_REQUESTED,  // Task requests execution
        EXECUTION_COMPLETED,  // Servant says that execution was completed
        EXECUTION_TIMEOUT,  // Executing longer than execution timeout
        DISCONNECTED,  // Scheduler lost connection to servant
        COMMUNICATION_COMPLETED,  // All data from servant was uploaded
        IDLE_TIMEOUT,  // Servant is too long unused
        SIGNAL,  // Process signal to servant
        STOP,  // Stop requested
        STOPPING_TIMEOUT,  // Timeout of graceful shutdown of servant
        STOPPED  // Servant exited
    }

    public static class EventBuilder {
        private final ServantState state;
        private final Type type;
        private LocalDateTime timestamp = LocalDateTime.now();
        private Integer rc = null;
        private String description = null;
        private String taskId = null;
        private Integer signalNumber = null;

        private EventBuilder(ServantState state, Type type) {
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

        public EventBuilder setTimestamp(LocalDateTime timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public EventBuilder setTaskId(String taskId) {
            this.taskId = taskId;
            return this;
        }

        public EventBuilder setSignalNumber(Integer signalNumber) {
            this.signalNumber = signalNumber;
            return this;
        }

        public ServantEvent build() {
            return new ServantEvent(
                UUID.randomUUID().toString(),
                timestamp,
                state.id(),
                state.workflowId(),
                type,
                description,
                rc,
                taskId,
                signalNumber
            );
        }
    }
}
