package ai.lzy.scheduler.worker.impl;

import ai.lzy.model.ReturnCodes;
import ai.lzy.model.db.exceptions.DaoException;
import ai.lzy.scheduler.allocator.WorkersAllocator;
import ai.lzy.scheduler.configs.WorkerEventProcessorConfig;
import ai.lzy.scheduler.db.TaskDao;
import ai.lzy.scheduler.db.WorkerDao;
import ai.lzy.scheduler.db.WorkerEventDao;
import ai.lzy.scheduler.models.WorkerEvent;
import ai.lzy.scheduler.models.WorkerEvent.Type;
import ai.lzy.scheduler.models.WorkerState;
import ai.lzy.scheduler.models.WorkerState.Status;
import ai.lzy.scheduler.task.Task;
import ai.lzy.scheduler.worker.Worker;
import ai.lzy.scheduler.worker.WorkerConnection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import javax.annotation.Nullable;

public class WorkerEventProcessor extends Thread {

    private static final Logger LOG = LogManager.getLogger(WorkerEventProcessor.class);
    private static final ThreadGroup WORKERS_TG = new ThreadGroup("workers");

    private final EventQueue queue;
    private final WorkerDao dao;
    private final WorkerEventDao eventDao;
    private final WorkerEventProcessorConfig config;
    private final WorkersAllocator allocator;
    private final TaskDao taskDao;
    private final String workerId;
    private final String workflowName;
    private final BiConsumer<String, String> notifyReady;  // notify scheduler about free worker
    private final BiConsumer<String, String> notifyDestroyed;  // notify scheduler about destroyed state
    private final AtomicBoolean stopping = new AtomicBoolean(false);

    @Nullable
    private WorkerConnection connection = null;

    public WorkerEventProcessor(String workflowName, String workerId,
                                WorkerEventProcessorConfig config, WorkersAllocator allocator, TaskDao taskDao,
                                WorkerEventDao eventDao, WorkerDao dao, EventQueueManager queueManager,
                                BiConsumer<String, String> notifyReady, BiConsumer<String, String> notifyDestroyed)
    {
        super(WORKERS_TG, "worker-" + workerId);
        this.dao = dao;
        this.notifyReady = notifyReady;
        this.config = config;
        this.allocator = allocator;
        this.taskDao = taskDao;
        this.eventDao = eventDao;
        this.workerId = workerId;
        this.workflowName = workflowName;
        this.notifyDestroyed = notifyDestroyed;
        queue = queueManager.get(workflowName, workerId);
    }

    @Override
    public void run() {
        try {
            while (!stopping.get()) {
                final WorkerEvent event;
                try {
                    event = queue.waitForNext();
                } catch (InterruptedException e) {
                    LOG.debug("Thread interrupted");
                    continue;
                } catch (Exception e) {
                    LOG.error("Error while waiting for events", e);
                    final WorkerState currentState;
                    try {
                        currentState = dao.acquire(workflowName, workerId);
                    } catch (WorkerDao.AcquireException | DaoException er) {
                        throw new RuntimeException("Cannot acquire worker for processing", er);  // must be unreachable
                    }
                    if (currentState == null) {
                        throw new IllegalStateException("Cannot find worker");  // Destroying this thread
                    }
                    try {
                        dao.updateAndFree(destroy(currentState, null));
                    } catch (DaoException ex) {
                        throw new RuntimeException("Cannot free worker from processing", ex);  // must be unreachable
                    }
                    throw e;
                }
                if (stopping.get()) {
                    queue.put(event);
                    return;
                }
                boolean isDestroyed = this.process(event);
                if (isDestroyed) {
                    notifyDestroyed.accept(workflowName, workerId);
                    return;
                }
            }
        } finally {
            destroy();
        }

    }

    private boolean process(WorkerEvent event) {

        final WorkerState currentState;
        try {
            currentState = dao.acquire(workflowName, workerId);
        } catch (Exception e) {
            this.notifyDestroyed.accept(workflowName, workerId);
            throw new RuntimeException("Cannot acquire worker for processing");  // must be unreachable
        }
        if (currentState == null) {
            this.notifyDestroyed.accept(workflowName, workerId);
            throw new IllegalStateException("Cannot find worker");  // Destroying this thread
        }
        WorkerState newState;
        try {
            newState = processEvent(currentState, event);
        } catch (Exception e) {
            LOG.error("Error while processing event {}.\n Current state: {}", event, currentState, e);
            newState = destroy(currentState, event);
        }
        try {
            dao.updateAndFree(newState);
            LOG.debug("Worker state processed.\n old: {}\n new: {}\n event: {}",
                    currentState, newState, event);
        } catch (DaoException e) {
            LOG.error("Cannot write new worker state to dao", e);
            throw new RuntimeException(e);
        }
        if ((newState.status() == Status.RUNNING || newState.status() == Status.IDLE)
            && !(currentState.status() == Status.RUNNING || currentState.status() == Status.IDLE))
        {
            notifyReady.accept(workflowName, workerId);
        }
        return newState.status() == Status.DESTROYED;
    }

    private WorkerState processEvent(WorkerState currentState,
                                     WorkerEvent event) throws AssertionException, DaoException
    {
        return switch (event.type()) {
            case ALLOCATION_TIMEOUT, STOPPING_TIMEOUT, STOPPED,
                    IDLE_HEARTBEAT_TIMEOUT, EXECUTING_HEARTBEAT_TIMEOUT -> destroy(currentState, event);

            case CONNECTED -> {
                assertStatus(currentState, event, Status.CONNECTING);
                eventDao.removeAllByTypes(currentState.id(), Type.ALLOCATION_TIMEOUT);
                if (event.workerUrl() == null) {
                    throw new AssertionException();
                }
                Worker worker = dao.get(workflowName, workerId);
                if (worker == null) {
                    throw new AssertionException();
                }
                this.connection = new WorkerConnectionImpl(event.workerUrl());
                final Task task = getTask(currentState.taskId());
                final WorkerConnection connection = getConnection(currentState);

                connection.api().configure(task.description().operation().env());
                final WorkerEvent timeout = WorkerEvent.fromState(currentState, Type.CONFIGURATION_TIMEOUT)
                    .setTimeout(config.configuringTimeout())
                    .setRc(ReturnCodes.ENVIRONMENT_INSTALLATION_ERROR.getRc())
                    .setDescription("Environment is installing too long.")
                    .build();
                queue.put(timeout);
                yield currentState.copy()
                    .setStatus(Status.CONFIGURING)
                    .setTaskId(task.taskId())
                    .setWorkerUrl(event.workerUrl())
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
                    final WorkerEvent timeout = WorkerEvent.fromState(currentState, Type.ALLOCATION_TIMEOUT)
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

                final WorkerConnection connection = getConnection(currentState);

                connection.api().configure(task.description().operation().env());
                final WorkerEvent timeout = WorkerEvent.fromState(currentState, Type.CONFIGURATION_TIMEOUT)
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
                    yield stop(currentState, "Error while configuring worker: " + event.description());
                }

                final Task task = getTask(currentState.taskId());
                final WorkerConnection connection = getConnection(currentState);

                connection.api().startExecution(currentState.taskId(), task.description());

                queue.put(WorkerEvent
                    .fromState(currentState, Type.EXECUTING_HEARTBEAT_TIMEOUT)
                    .setRc(ReturnCodes.INTERNAL_ERROR.getRc())
                    .setDescription("Worker is dead")
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
                queue.put(WorkerEvent
                    .fromState(currentState, Type.EXECUTING_HEARTBEAT_TIMEOUT)
                    .setRc(ReturnCodes.INTERNAL_ERROR.getRc())
                    .setDescription("Worker is dead")
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
                queue.put(WorkerEvent
                    .fromState(currentState, Type.IDLE_HEARTBEAT_TIMEOUT)
                    .setTimeout(config.idleHeartbeatPeriod())
                    .build());
                queue.put(WorkerEvent
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
                queue.put(WorkerEvent
                    .fromState(currentState, Type.IDLE_HEARTBEAT_TIMEOUT)
                    .setTimeout(config.idleHeartbeatPeriod())
                    .setRc(ReturnCodes.INTERNAL_ERROR.getRc())
                    .setDescription("Worker is dead")
                    .build()
                );
                yield currentState;
            }

            case COMMUNICATION_COMPLETED -> {
                assertStatus(currentState, event, Status.RUNNING);
                yield idle(currentState);
            }

            case STOP -> stop(currentState, "Worker stopping: <" + event.description() + ">");

            case IDLE_TIMEOUT -> {
                assertStatus(currentState, event, Status.IDLE, Status.RUNNING);
                yield stop(currentState, "Worker stopping: <" + event.description() + ">");
            }

            case EXECUTION_TIMEOUT -> {
                assertStatus(currentState, event, Status.EXECUTING);
                yield stop(currentState, "Worker stopping: <" + event.description() + ">");
            }

            case CONFIGURATION_TIMEOUT -> {
                assertStatus(currentState, event, Status.CONFIGURING);
                yield stop(currentState, "Worker stopping: <" + event.description() + ">");
            }

            case NOOP -> currentState;
        };
    }

    private WorkerConnection getConnection(WorkerState currentState) throws AssertionException {
        if (connection == null) {
            if (currentState.workerUrl() == null) {
                throw new AssertionException();
            }
            Worker worker = null;
            try {
                worker = dao.get(workflowName, workerId);
            } catch (DaoException e) {
                LOG.error("Cannot get worker", e);
            }
            if (worker == null) {
                throw new AssertionException();
            }
            connection = new WorkerConnectionImpl(currentState.workerUrl());
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

    private WorkerState destroy(WorkerState currentState, @Nullable WorkerEvent currentEvent) {
        if (currentEvent != null) {
            LOG.info("Destroying worker <{}> from workflow <{}> because of event <{}>, description <{}>",
                    currentState.id(), currentState.workflowName(), currentEvent.type(), currentEvent.description());
        }
        try {
            allocator.free(currentState.workflowName(), currentState.id());
        } catch (Exception e) {
            throw new RuntimeException("Exception while destroying worker", e);
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
            && !currentEvent.taskId().equals(currentState.taskId()))
        {
            try {
                final Task task = taskDao.get(currentEvent.taskId());
                if (task != null) {
                    task.notifyExecutionCompleted(rc, description);
                }
            } catch (DaoException e) {
                LOG.error("Cannot get task from dao", e);
            }
        }

        final String workerExitDescription = currentState.errorDescription() != null
            ? currentState.errorDescription()
            : description;

        return currentState.copy()
            .setStatus(Status.DESTROYED)
            .setTaskId(null)
            .setErrorDescription(workerExitDescription)
            .build();
    }

    private void destroy() {
        if (this.connection != null) {
            this.connection.close();
        }
    }

    private void assertStatus(WorkerState currentState, WorkerEvent event,
                              Status... statuses) throws AssertionException
    {
        if (!List.of(statuses).contains(currentState.status())) {
            LOG.error("Status <{}> is not acceptable for event <{}>", currentState.status(), event);
            throw new AssertionException();
        }
    }

    private WorkerState changeStatus(WorkerState currentState, Status status) {
        return currentState.copy()
            .setStatus(status)
            .build();
    }

    private WorkerState idle(WorkerState currentState) {
        final WorkerEvent timeout = WorkerEvent.fromState(currentState, Type.IDLE_TIMEOUT)
            .setTimeout(config.idleTimeout())
            .setRc(ReturnCodes.SUCCESS.getRc())
            .setDescription("Worker is destroyed because of long idle state")
            .build();
        queue.put(timeout);
        return changeStatus(currentState, Status.IDLE);
    }

    private WorkerState stop(WorkerState currentState, String description) throws AssertionException {
        eventDao.removeAll(currentState.id());
        if (currentState.taskId() != null) {
            Task task = getTask(currentState.taskId());
            try {
                task.notifyExecutionCompleted(ReturnCodes.INTERNAL_ERROR.getRc(), description);
            } catch (DaoException e) {
                LOG.error("Cannot notify task about stop", e);
            }
        }
        final WorkerConnection connection = getConnection(currentState);
        connection.api().stop();
        final WorkerEvent timeout = WorkerEvent.fromState(currentState, Type.STOPPING_TIMEOUT)
            .setTimeout(config.workerStopTimeout())
            .setRc(ReturnCodes.INTERNAL_ERROR.getRc())
            .setDescription("Worker stopping timeout")
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
        this.queue.put(WorkerEvent.noop(workflowName, workerId));
    }
}
