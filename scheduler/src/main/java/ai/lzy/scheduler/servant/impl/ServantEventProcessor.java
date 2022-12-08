package ai.lzy.scheduler.servant.impl;

import ai.lzy.model.ReturnCodes;
import ai.lzy.model.db.exceptions.DaoException;
import ai.lzy.scheduler.allocator.ServantsAllocator;
import ai.lzy.scheduler.configs.ServantEventProcessorConfig;
import ai.lzy.scheduler.db.ServantDao;
import ai.lzy.scheduler.db.ServantEventDao;
import ai.lzy.scheduler.db.TaskDao;
import ai.lzy.scheduler.models.ServantEvent;
import ai.lzy.scheduler.models.ServantEvent.Type;
import ai.lzy.scheduler.models.ServantState;
import ai.lzy.scheduler.models.ServantState.Status;
import ai.lzy.scheduler.servant.Servant;
import ai.lzy.scheduler.servant.ServantConnection;
import ai.lzy.scheduler.task.Task;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import javax.annotation.Nullable;

public class ServantEventProcessor extends Thread {

    private static final Logger LOG = LogManager.getLogger(ServantEventProcessor.class);
    private static final ThreadGroup SERVANTS_TG = new ThreadGroup("servants");

    private final EventQueue queue;
    private final ServantDao dao;
    private final ServantEventDao eventDao;
    private final ServantEventProcessorConfig config;
    private final ServantsAllocator allocator;
    private final TaskDao taskDao;
    private final String servantId;
    private final String workflowName;
    private final BiConsumer<String, String> notifyReady;  // notify scheduler about free servant
    private final BiConsumer<String, String> notifyDestroyed;  // notify scheduler about destroyed state
    private final AtomicBoolean stopping = new AtomicBoolean(false);

    @Nullable
    private ServantConnection connection = null;

    public ServantEventProcessor(String workflowName, String servantId,
                                 ServantEventProcessorConfig config, ServantsAllocator allocator, TaskDao taskDao,
                                 ServantEventDao eventDao, ServantDao dao, EventQueueManager queueManager,
                                 BiConsumer<String, String> notifyReady, BiConsumer<String, String> notifyDestroyed) {
        super(SERVANTS_TG, "servant-" + servantId);
        this.dao = dao;
        this.notifyReady = notifyReady;
        this.config = config;
        this.allocator = allocator;
        this.taskDao = taskDao;
        this.eventDao = eventDao;
        this.servantId = servantId;
        this.workflowName = workflowName;
        this.notifyDestroyed = notifyDestroyed;
        queue = queueManager.get(workflowName, servantId);
    }

    @Override
    public void run() {
        try {
            while (!stopping.get()) {
                final ServantEvent event;
                try {
                    event = queue.waitForNext();
                } catch (InterruptedException e) {
                    LOG.debug("Thread interrupted");
                    continue;
                } catch (Exception e) {
                    LOG.error("Error while waiting for events", e);
                    final ServantState currentState;
                    try {
                        currentState = dao.acquire(workflowName, servantId);
                    } catch (ServantDao.AcquireException | DaoException er) {
                        throw new RuntimeException("Cannot acquire servant for processing", er);  // must be unreachable
                    }
                    if (currentState == null) {
                        throw new IllegalStateException("Cannot find servant");  // Destroying this thread
                    }
                    try {
                        dao.updateAndFree(destroy(currentState, null));
                    } catch (DaoException ex) {
                        throw new RuntimeException("Cannot free servant from processing", ex);  // must be unreachable
                    }
                    throw e;
                }
                if (stopping.get()) {
                    queue.put(event);
                    return;
                }
                boolean isDestroyed = this.process(event);
                if (isDestroyed) {
                    notifyDestroyed.accept(workflowName, servantId);
                    return;
                }
            }
        } finally {
            destroy();
        }

    }

    private boolean process(ServantEvent event) {

        final ServantState currentState;
        try {
            currentState = dao.acquire(workflowName, servantId);
        } catch (ServantDao.AcquireException | DaoException e) {
            throw new RuntimeException("Cannot acquire servant for processing");  // must be unreachable
        }
        if (currentState == null) {
            throw new IllegalStateException("Cannot find servant");  // Destroying this thread
        }
        ServantState newState;
        try {
            newState = processEvent(currentState, event);
        } catch (Exception e) {
            LOG.error("Error while processing event {}.\n Current state: {}", event, currentState, e);
            newState = destroy(currentState, event);
        }
        try {
            dao.updateAndFree(newState);
            LOG.debug("Servant state processed.\n old: {}\n new: {}\n event: {}",
                    currentState, newState, event);
        } catch (DaoException e) {
            LOG.error("Cannot write new servant state to dao", e);
            throw new RuntimeException(e);
        }
        if ((newState.status() == Status.RUNNING || newState.status() == Status.IDLE)
            && !(currentState.status() == Status.RUNNING || currentState.status() == Status.IDLE)) {
            notifyReady.accept(workflowName, servantId);
        }
        return newState.status() == Status.DESTROYED;
    }

    private ServantState processEvent(ServantState currentState,
                                      ServantEvent event) throws AssertionException, DaoException {
        return switch (event.type()) {
            case ALLOCATION_TIMEOUT, STOPPING_TIMEOUT, STOPPED,
                    IDLE_HEARTBEAT_TIMEOUT, EXECUTING_HEARTBEAT_TIMEOUT -> destroy(currentState, event);

            case CONNECTED -> {
                assertStatus(currentState, event, Status.CONNECTING);
                eventDao.removeAllByTypes(currentState.id(), Type.ALLOCATION_TIMEOUT);
                if (event.servantUrl() == null) {
                    throw new AssertionException();
                }
                Servant servant = dao.get(workflowName, servantId);
                if (servant == null) {
                    throw new AssertionException();
                }
                this.connection = new ServantConnectionImpl(event.servantUrl());
                final Task task = getTask(currentState.taskId());
                final ServantConnection connection = getConnection(currentState);

                connection.api().configure(task.description().operation().env());
                final ServantEvent timeout = ServantEvent.fromState(currentState, Type.CONFIGURATION_TIMEOUT)
                    .setTimeout(config.configuringTimeout())
                    .setRc(ReturnCodes.ENVIRONMENT_INSTALLATION_ERROR.getRc())
                    .setDescription("Environment is installing too long.")
                    .build();
                queue.put(timeout);
                yield currentState.copy()
                    .setStatus(Status.CONFIGURING)
                    .setTaskId(task.taskId())
                    .setServantUrl(event.servantUrl())
                    .build();
            }

            case EXECUTION_REQUESTED -> {
                assertStatus(currentState, event, Status.RUNNING, Status.IDLE, Status.CREATED);
                eventDao.removeAllByTypes(currentState.id(), Type.IDLE_TIMEOUT);
                eventDao.removeAllByTypes(currentState.id(), Type.IDLE_HEARTBEAT_TIMEOUT, Type.IDLE_HEARTBEAT);
                if (event.taskId() == null) {
                    LOG.error("Execute event without taskId: {}", event);
                    throw new AssertionException();
                }

                final Task task = getTask(event.taskId());
                task.notifyExecuting(currentState.id());

                if (currentState.status() == Status.CREATED) {
                    allocator.allocate(currentState.userId(), currentState.workflowName(),
                        currentState.id(), currentState.requirements());
                    final ServantEvent timeout = ServantEvent.fromState(currentState, Type.ALLOCATION_TIMEOUT)
                        .setTimeout(config.allocationTimeout())
                        .setRc(ReturnCodes.INTERNAL_ERROR.getRc())
                        .setDescription("Allocation timeout reached")
                        .build();
                    queue.put(timeout);
                    yield currentState.copy()
                        .setStatus(Status.CONNECTING)
                        .setTaskId(task.taskId())
                        .build();
                }

                final ServantConnection connection = getConnection(currentState);

                connection.api().configure(task.description().operation().env());
                final ServantEvent timeout = ServantEvent.fromState(currentState, Type.CONFIGURATION_TIMEOUT)
                    .setTimeout(config.configuringTimeout())
                    .setRc(ReturnCodes.ENVIRONMENT_INSTALLATION_ERROR.getRc())
                    .setDescription("Environment is installing too long.")
                    .build();
                queue.put(timeout);
                yield currentState.copy()
                    .setStatus(Status.CONFIGURING)
                    .setTaskId(task.taskId())
                    .build();
            }

            case CONFIGURED -> {
                assertStatus(currentState, event, Status.CONFIGURING);
                eventDao.removeAllByTypes(currentState.id(), Type.CONFIGURATION_TIMEOUT);
                if (event.rc() != ReturnCodes.SUCCESS.getRc()) {
                    yield stop(currentState, "Error while configuring servant: " + event.description());
                }

                final Task task = getTask(currentState.taskId());
                final ServantConnection connection = getConnection(currentState);

                connection.api().startExecution(currentState.taskId(), task.description());

                queue.put(ServantEvent
                    .fromState(currentState, Type.EXECUTING_HEARTBEAT_TIMEOUT)
                    .setRc(ReturnCodes.INTERNAL_ERROR.getRc())
                    .setDescription("Servant is dead")
                    .setTimeout(config.executingHeartbeatPeriod())
                    .build()
                );
                yield currentState.copy()
                    .setStatus(Status.EXECUTING)
                    .build();
            }

            case EXECUTING_HEARTBEAT -> {
                assertStatus(currentState, event, Status.EXECUTING);
                eventDao.removeAllByTypes(currentState.id(), Type.EXECUTING_HEARTBEAT_TIMEOUT);
                queue.put(ServantEvent
                    .fromState(currentState, Type.EXECUTING_HEARTBEAT_TIMEOUT)
                    .setRc(ReturnCodes.INTERNAL_ERROR.getRc())
                    .setDescription("Servant is dead")
                    .setTimeout(config.executingHeartbeatPeriod())
                    .build()
                );
                yield currentState;
            }

            case EXECUTION_COMPLETED -> {
                assertStatus(currentState, event, Status.EXECUTING);
                eventDao.removeAllByTypes(currentState.id(),
                        Type.EXECUTING_HEARTBEAT_TIMEOUT, Type.EXECUTING_HEARTBEAT);
                final Task task = getTask(currentState.taskId());
                task.notifyExecutionCompleted(event.rc(), event.description());
                queue.put(ServantEvent
                    .fromState(currentState, Type.IDLE_HEARTBEAT_TIMEOUT)
                    .setTimeout(config.idleHeartbeatPeriod())
                    .build());
                queue.put(ServantEvent
                    .fromState(currentState, Type.IDLE_TIMEOUT)
                    .setTimeout(config.idleTimeout())
                    .build());
                yield currentState.copy()
                    .setStatus(Status.RUNNING)
                    .setTaskId(null)
                    .build();
            }

            case IDLE_HEARTBEAT -> {
                assertStatus(currentState, event, Status.IDLE, Status.RUNNING, Status.CONFIGURING);
                eventDao.removeAllByTypes(currentState.id(), Type.IDLE_HEARTBEAT_TIMEOUT);
                queue.put(ServantEvent
                    .fromState(currentState, Type.IDLE_HEARTBEAT_TIMEOUT)
                    .setTimeout(config.idleHeartbeatPeriod())
                    .setRc(ReturnCodes.INTERNAL_ERROR.getRc())
                    .setDescription("Servant is dead")
                    .build()
                );
                yield currentState;
            }

            case COMMUNICATION_COMPLETED -> {
                assertStatus(currentState, event, Status.RUNNING);
                yield idle(currentState);
            }

            case STOP -> stop(currentState, "Servant stopping: <" + event.description() + ">");

            case IDLE_TIMEOUT -> {
                assertStatus(currentState, event, Status.IDLE, Status.RUNNING);
                yield stop(currentState, "Servant stopping: <" + event.description() + ">");
            }

            case EXECUTION_TIMEOUT -> {
                assertStatus(currentState, event, Status.EXECUTING);
                yield stop(currentState, "Servant stopping: <" + event.description() + ">");
            }

            case CONFIGURATION_TIMEOUT -> {
                assertStatus(currentState, event, Status.CONFIGURING);
                yield stop(currentState, "Servant stopping: <" + event.description() + ">");
            }

            case NOOP -> currentState;
        };
    }

    private ServantConnection getConnection(ServantState currentState) throws AssertionException {
        if (connection == null) {
            if (currentState.servantUrl() == null) {
                throw new AssertionException();
            }
            Servant servant = null;
            try {
                servant = dao.get(workflowName, servantId);
            } catch (DaoException e) {
                LOG.error("Cannot get servant", e);
            }
            if (servant == null) {
                throw new AssertionException();
            }
            connection = new ServantConnectionImpl(currentState.servantUrl());
        }
        return connection;
    }

    private Task getTask(String taskId) throws AssertionException {
        final Task task;
        try {
            task = taskDao.get(taskId);
        } catch (DaoException e) {
            LOG.error("Cannot get task from dao", e);
            throw new AssertionException();
        }
        if (task == null) {
            throw new AssertionException();
        }
        return task;
    }

    private ServantState destroy(ServantState currentState, @Nullable ServantEvent currentEvent) {
        if (currentEvent != null) {
            LOG.info("Destroying servant <{}> from workflow <{}> because of event <{}>, description <{}>",
                    currentState.id(), currentState.workflowName(), currentEvent.type(), currentEvent.description());
        }
        try {
            allocator.free(currentState.workflowName(), currentState.id());
        } catch (Exception e) {
            throw new RuntimeException("Exception while destroying servant", e);
        }
        eventDao.removeAll(currentState.id());

        final int rc;
        final String description;
        if (currentEvent != null && currentEvent.rc() != null && currentEvent.rc() != ReturnCodes.SUCCESS.getRc()) {
            rc = currentEvent.rc();
            description = currentEvent.description() == null ? "Internal error" : currentEvent.description();
        } else {
            rc = ReturnCodes.INTERNAL_ERROR.getRc();
            description = "Internal error";
        }

        if (connection != null) {
            connection.close();
            connection = null;
        }

        if (currentState.taskId() != null) {
            try {
                final Task task = taskDao.get(currentState.taskId());
                if (task != null) {
                    task.notifyExecutionCompleted(rc, description);
                }
            } catch (DaoException e) {
                LOG.error("Cannot get task from dao", e);
            }
        }

        if (currentEvent != null
            && currentEvent.taskId() != null
            && !currentEvent.taskId().equals(currentState.taskId())) {
            try {
                final Task task = taskDao.get(currentEvent.taskId());
                if (task != null) {
                    task.notifyExecutionCompleted(rc, description);
                }
            } catch (DaoException e) {
                LOG.error("Cannot get task from dao", e);
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

    private void destroy() {
        if (this.connection != null) {
            this.connection.close();
        }
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
            .setTimeout(config.idleTimeout())
            .setRc(ReturnCodes.SUCCESS.getRc())
            .setDescription("Servant is destroyed because of long idle state")
            .build();
        queue.put(timeout);
        return changeStatus(currentState, Status.IDLE);
    }

    private ServantState stop(ServantState currentState, String description) throws AssertionException {
        eventDao.removeAll(currentState.id());
        if (currentState.taskId() != null) {
            Task task = getTask(currentState.taskId());
            try {
                task.notifyExecutionCompleted(ReturnCodes.INTERNAL_ERROR.getRc(), description);
            } catch (DaoException e) {
                LOG.error("Cannot notify task about stop", e);
            }
        }
        final ServantConnection connection = getConnection(currentState);
        connection.api().stop();
        final ServantEvent timeout = ServantEvent.fromState(currentState, Type.STOPPING_TIMEOUT)
            .setTimeout(config.servantStopTimeout())
            .setRc(ReturnCodes.INTERNAL_ERROR.getRc())
            .setDescription("Servant stopping timeout")
            .build();
        queue.put(timeout);
        return currentState.copy()
            .setStatus(Status.STOPPING)
            .setErrorDescription(description)
            .setTaskId(null)
            .build();
    }

    private static class AssertionException extends Exception {}

    public void shutdown() {
        this.stopping.set(true);
        this.queue.put(ServantEvent.noop(workflowName, servantId));
    }
}
