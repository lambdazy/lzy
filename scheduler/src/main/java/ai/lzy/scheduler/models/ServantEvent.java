package ai.lzy.scheduler.models;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

import org.apache.curator.shaded.com.google.common.net.HostAndPort;
import org.jetbrains.annotations.NotNull;

public record ServantEvent(
    String id,
    Instant timestamp,
    String servantId,
    String workflowName,
    Type type,

    @Nullable String description,
    @Nullable Integer rc,
    @Nullable String taskId,
    @Nullable HostAndPort servantUrl
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
        return unit.toChronoUnit().between(Instant.now(), timestamp);
    }

    public enum Type {
        ALLOCATION_TIMEOUT,
        CONNECTED,  // Servant connected to scheduler with method register
        CONFIGURED,  // Servant env configuration completed
        CONFIGURATION_TIMEOUT,  // Servant configuration timed out
        EXECUTION_REQUESTED,  // Task requests execution
        EXECUTING_HEARTBEAT,  // Heartbeat of servant while its executing task
        EXECUTING_HEARTBEAT_TIMEOUT,  // Timeout of heartbeat waiting of servant
        EXECUTION_COMPLETED,  // Servant says that execution was completed
        EXECUTION_TIMEOUT,  // Executing longer than execution timeout
        COMMUNICATION_COMPLETED,  // All data from servant was uploaded
        IDLE_HEARTBEAT,  // Heartbeat of servant while its idle
        IDLE_HEARTBEAT_TIMEOUT,  // Timeout of heartbeat waiting of servant
        IDLE_TIMEOUT,  // Servant is too long unused
        STOP,  // Stop requested
        STOPPING_TIMEOUT,  // Timeout of graceful shutdown of servant
        STOPPED  // Servant exited
    }

    @Override
    public String toString() {
        return String.format("<type:%s, timestamp: %s>", type, timestamp);
    }

    public static class EventBuilder {
        private final ServantState state;
        private final Type type;
        private Instant timestamp = Instant.now();
        private Integer rc = null;
        private String description = null;
        private String taskId = null;
        private HostAndPort servantUrl = null;

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

        public EventBuilder setTimeout(long timeoutSecs) {
            this.timestamp = Instant.now().plus(timeoutSecs, ChronoUnit.SECONDS);
            return this;
        }

        public EventBuilder setTaskId(String taskId) {
            this.taskId = taskId;
            return this;
        }

        public EventBuilder setServantUrl(HostAndPort servantUrl) {
            this.servantUrl = servantUrl;
            return this;
        }

        public ServantEvent build() {
            return new ServantEvent(
                UUID.randomUUID().toString(),
                timestamp,
                state.id(),
                state.workflowName(),
                type,
                description,
                rc,
                taskId,
                servantUrl
            );
        }
    }
}
