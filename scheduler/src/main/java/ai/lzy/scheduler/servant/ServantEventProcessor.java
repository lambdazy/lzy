package ai.lzy.scheduler.servant;

import ai.lzy.scheduler.allocator.ServantsAllocator;
import ai.lzy.scheduler.configs.ServantEventProcessorConfig;
import ai.lzy.scheduler.db.ServantEventDao;
import ai.lzy.scheduler.db.TaskDao;
import ai.lzy.scheduler.models.ServantEvent;
import ai.lzy.scheduler.models.ServantEvent.Type;
import ai.lzy.scheduler.models.ServantState;
import ai.lzy.scheduler.models.ServantState.Status;
import ai.lzy.scheduler.servant.ServantConnectionManager.ServantConnection;
import ai.lzy.scheduler.task.Task;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.ReturnCodes;

// TODO(artolord) Add more logs

@Singleton
public class ServantEventProcessor {
    private final EventQueue queue;
    private final ServantEventDao eventDao;
    private final ServantEventProcessorConfig config;
    private final ServantsAllocator allocator;
    private final ServantConnectionManager connections;
    private final TaskDao taskDao;
    private static final Logger LOG = LogManager.getLogger(ServantEventProcessor.class);

    @Inject
    public ServantEventProcessor(EventQueue queue, ServantEventProcessorConfig config,
                                 ServantsAllocator allocator, ServantConnectionManager connections,
                                 TaskDao taskDao, ServantEventDao eventDao) {
        this.queue = queue;
        this.config = config;
        this.allocator = allocator;
        this.connections = connections;
        this.taskDao = taskDao;
        this.eventDao = eventDao;
    }

    public ServantState process(ServantState currentState, ServantEvent event) {
        try {
            return processEvent(currentState, event);
        } catch (AssertionException e) {
            LOG.error("Wrong servant state", e);
            return destroy(currentState, event);
        }
    }

    private ServantState processEvent(ServantState currentState, ServantEvent event) throws AssertionException {
        return switch (event.type()) {
            case ALLOCATION_TIMEOUT, CONFIGURATION_TIMEOUT,
                EXECUTION_TIMEOUT, IDLE_TIMEOUT, STOPPING_TIMEOUT,
                DISCONNECTED, STOPPED -> destroy(currentState, event);

            case ALLOCATION_REQUESTED -> {
                assertStatus(currentState, event, Status.CREATED);
                allocator.allocate(
                    currentState.workflowId(), currentState.id(),
                    currentState.provisioning(), currentState.env()
                );
                final ServantEvent timeout = ServantEvent.fromState(currentState, Type.ALLOCATION_TIMEOUT)
                    .setTimestamp(LocalDateTime.now().plus(config.allocationTimeoutSeconds(), ChronoUnit.SECONDS))
                    .setRc(ReturnCodes.INTERNAL.getRc())
                    .setDescription("Allocation timeout reached")
                    .build();
                queue.put(timeout);
                yield changeStatus(currentState, Status.CONNECTING);
            }

            case CONNECTED -> {
                assertStatus(currentState, event, Status.CONNECTING);
                eventDao.removeAll(currentState.id(), Type.ALLOCATION_TIMEOUT);
                try (final ServantConnection connection = getConnection(currentState)) {
                    connection.api().configure(currentState.env());
                }
                yield configuring(currentState);
            }

            case CONFIGURED -> {
                assertStatus(currentState, event, Status.CONFIGURING);
                eventDao.removeAll(currentState.id(), Type.CONFIGURATION_TIMEOUT);
                yield idle(currentState);
            }

            case EXECUTION_REQUESTED -> {
                assertStatus(currentState, event, Status.RUNNING, Status.IDLE);
                if (currentState.status() == Status.IDLE) {
                    eventDao.removeAll(currentState.id(), Type.IDLE_TIMEOUT);
                }
                if (event.taskId() == null) {
                    throw new AssertionException();
                }

                final Task task = getTask(currentState.workflowId(), event.taskId());

                try (final ServantConnection connection = getConnection(currentState)) {
                    connection.api().startExecution(task.description());
                }
                task.notifyExecuting(currentState.id());
                yield currentState.copy()
                    .setStatus(Status.EXECUTING)
                    .setTaskId(event.taskId())
                    .build();
            }

            case EXECUTION_COMPLETED -> {
                assertStatus(currentState, event, Status.EXECUTING);
                final Task task = getTask(currentState.workflowId(), currentState.taskId());
                task.notifyExecutionCompleted(event.rc(), event.description());
                yield currentState.copy()
                    .setStatus(Status.RUNNING)
                    .setTaskId(null)
                    .build();
            }

            case COMMUNICATION_COMPLETED -> {
                assertStatus(currentState, event, Status.RUNNING);
                yield idle(currentState);
            }

            case STOP -> {
                try (final ServantConnection connection = getConnection(currentState)) {
                    connection.api().gracefulStop();
                }
                yield stop(currentState, "Servant stopping: <" + event.description() + ">");
            }

            case SIGNAL -> {
                Integer signal = event.signalNumber();
                if (signal == null) {
                    yield currentState;
                }
                try (final ServantConnection connection = getConnection(currentState)) {
                    connection.api().signal(signal);
                }
                yield currentState;
            }
        };
    }

    private ServantConnection getConnection(ServantState currentState) throws AssertionException {
        final ServantConnection connection = connections.get(currentState.workflowId(), currentState.id());
        if (connection == null) {
            throw new AssertionException();
        }
        return connection;
    }

    private Task getTask(String workflowId, String taskId) throws AssertionException {
        final Task task = taskDao.get(workflowId, taskId);
        if (task == null) {
            throw new AssertionException();
        }
        return task;
    }

    private ServantState destroy(ServantState currentState, ServantEvent currentEvent) {
        allocator.destroy(currentState.workflowId(), currentState.id());
        eventDao.removeAll(currentState.id());
        final int rc = currentEvent.rc() == null ? ReturnCodes.INTERNAL.getRc() : currentEvent.rc();
        final String description = currentEvent.description() == null ? "Internal error" : currentEvent.description();

        if (currentState.taskId() != null) {
            final Task task = taskDao.get(currentState.workflowId(), currentState.taskId());
            if (task != null) {
                task.notifyExecutionCompleted(rc, description);
            }
        }

        if (currentEvent.taskId() != null && !currentEvent.taskId().equals(currentState.taskId())) {
            final Task task = taskDao.get(currentState.workflowId(), currentEvent.taskId());
            if (task != null) {
                task.notifyExecutionCompleted(rc, description);
            }
        }

        final String servantExitDescription = currentState.errorDescription() != null
            ? currentState.errorDescription()
            : description;

        return currentState.copy()
            .setStatus(Status.DESTROYED)
            .setTaskId(null)
            .setErrorDescription(servantExitDescription)
            .build();
    }

    private void assertStatus(ServantState currentState, ServantEvent event,
                                      Status... statuses) throws AssertionException {
        if (!List.of(statuses).contains(currentState.status())) {
            LOG.error("Status <{}> is not acceptable for event <{}>", currentState.status(), event);
            throw new AssertionException();
        }
    }

    private ServantState changeStatus(ServantState currentState, Status status) {
        return currentState.copy()
            .setStatus(status)
            .build();
    }

    private ServantState idle(ServantState currentState) {
        final ServantEvent timeout = ServantEvent.fromState(currentState, Type.IDLE_TIMEOUT)
            .setTimestamp(LocalDateTime.now().plus(config.idleTimeoutSeconds(), ChronoUnit.SECONDS))
            .setRc(ReturnCodes.SUCCESS.getRc())
            .setDescription("Servant is destroyed because of long idle state")
            .build();
        queue.put(timeout);
        return changeStatus(currentState, Status.IDLE);
    }

    private ServantState configuring(ServantState currentState) {
        final ServantEvent timeout = ServantEvent.fromState(currentState, Type.CONFIGURATION_TIMEOUT)
            .setTimestamp(LocalDateTime.now().plus(config.idleTimeoutSeconds(), ChronoUnit.SECONDS))
            .setRc(ReturnCodes.ENVIRONMENT_INSTALLATION_ERROR.getRc())
            .setDescription("Environment is installing too long.")
            .build();
        queue.put(timeout);
        return changeStatus(currentState, Status.CONFIGURING);
    }

    private ServantState stop(ServantState currentState, String description) {
        final ServantEvent timeout = ServantEvent.fromState(currentState, Type.STOPPING_TIMEOUT)
            .setTimestamp(LocalDateTime.now().plus(config.servantStopTimeoutSeconds(), ChronoUnit.SECONDS))
            .setRc(ReturnCodes.INTERNAL.getRc())
            .setDescription("Servant stopping timeout")
            .build();
        queue.put(timeout);
        return currentState.copy()
            .setStatus(Status.STOPPING)
            .setErrorDescription(description)
            .build();
    }

    private static class AssertionException extends Exception {}
}
