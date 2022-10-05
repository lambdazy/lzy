package ai.lzy.scheduler.servant.impl;

import ai.lzy.model.ReturnCodes;
import ai.lzy.model.TaskDesc;
import ai.lzy.model.db.exceptions.DaoException;
import ai.lzy.model.slot.Slot;
import ai.lzy.scheduler.configs.ServiceConfig;
import ai.lzy.scheduler.db.ServantDao;
import ai.lzy.scheduler.db.TaskDao;
import ai.lzy.scheduler.models.TaskState;
import ai.lzy.scheduler.servant.Scheduler;
import ai.lzy.scheduler.servant.Servant;
import ai.lzy.scheduler.servant.ServantsPool;
import ai.lzy.scheduler.task.Task;
import io.grpc.Status;
import io.grpc.StatusException;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

@Singleton
public class SchedulerImpl extends Thread implements Scheduler {
    private static final Logger LOG = LogManager.getLogger(SchedulerImpl.class);

    private final ServantDao dao;
    private final TaskDao taskDao;
    private final ServantsPool pool;
    private final BlockingQueue<Task> tasks = new LinkedBlockingQueue<>();
    private final AtomicBoolean stopping = new AtomicBoolean(false);
    private final ServiceConfig config;

    @Inject
    public SchedulerImpl(ServantDao dao, TaskDao taskDao, ServantsPool pool, ServiceConfig config) {
        this.dao = dao;
        this.taskDao = taskDao;
        this.pool = pool;
        this.config = config;
        restore();
    }

    @Override
    public Task execute(String workflowId, String workflowName, String userId, TaskDesc taskDesc)
        throws StatusException
    {
        validateTask(taskDesc);
        if (stopping.get()) {
            throw io.grpc.Status.UNAVAILABLE.withDescription("Service is stopping. Please try again").asException();
        }
        try {
            final Task task = taskDao.create(workflowId, workflowName, userId, taskDesc);
            tasks.add(task);
            return task;
        } catch (DaoException e) {
            LOG.error("Error while creating task", e);
            throw io.grpc.Status.INTERNAL.withDescription("Cannot create task").asException();
        }
    }

    @Override
    public Task stopTask(String workflowId, String taskId, String issue) throws StatusException {
        if (stopping.get()) {
            throw io.grpc.Status.UNAVAILABLE.withDescription("Service is stopping. Please try again").asException();
        }
        final Task task = getTask(workflowId, taskId);

        if (task.servantId() == null) {
            throw io.grpc.Status.FAILED_PRECONDITION.withDescription("Task is not assigned to servant")
                .asException();
        }

        final Servant servant;
        try {
            servant = dao.get(workflowId, task.servantId());
        } catch (DaoException e) {
            LOG.error("Error while getting servant from dao", e);
            throw Status.INTERNAL.withDescription("Something wrong with service").asException();
        }
        if (servant == null) {
            throw io.grpc.Status.INTERNAL.withDescription("Task is assigned to servant, but servant is not exists")
                .asException();
        }

        servant.stop(issue);
        return task;
    }

    @Override
    public Task status(String workflowId, String taskId) throws StatusException {
        if (stopping.get()) {
            throw io.grpc.Status.UNAVAILABLE.withDescription("Service is stopping. Please try again").asException();
        }

        return getTask(workflowId, taskId);
    }

    @Override
    public void killAll(String workflowName, String issue) throws StatusException {
        try {
            List<Servant> servants = dao.get(workflowName);
            servants.forEach(s -> s.stop(issue));
        } catch (DaoException e) {
            LOG.error("Error while getting servant from db", e);
            throw Status.INTERNAL.asException();
        }
    }

    @Override
    public List<Task> list(String workflow) throws StatusException {
        try {
            return taskDao.list(workflow);
        } catch (DaoException e) {
            throw Status.INTERNAL.asException();
        }
    }

    @Override
    public void terminate() {
        stopping.set(true);
        pool.shutdown();
        tasks.add(Task.NOOP);
    }

    @Override
    public void awaitTermination() throws InterruptedException {
        pool.waitForShutdown();
        this.join();
    }

    @Override
    public void run() {
        while (!stopping.get()) {
            final Task task;
            try {
                task = tasks.take();
                if (task == Task.NOOP) {
                    continue;
                }
            } catch (InterruptedException e) {
                LOG.debug("Thread interrupted", e);
                continue;
            }

            var future = pool.waitForFree(task.userId(), task.workflowName(),
                task.description().operation().requirements());
            if (future == null) {
                LOG.info("Pool is stopping.");
                continue;
            }
            future.whenComplete((servant, throwable) -> {
                if (throwable != null) {
                    LOG.error("Unknown error", throwable);
                    try {
                        task.notifyExecutionCompleted(ReturnCodes.INTERNAL_ERROR.getRc(), "Some internal error");
                    } catch (DaoException e) {
                        LOG.error("Cannot notify task about error", e);
                    }
                    return;
                }
                try {
                    task.notifyScheduled();
                } catch (DaoException e) {
                    LOG.error("Cannot notify task about scheduled state");
                    throw new RuntimeException(e);
                }
                servant.setTask(task);
            });
        }
    }

    private void restore() {
        try {
            List<Task> queue = taskDao.filter(TaskState.Status.QUEUE);
            tasks.addAll(queue);
        } catch (DaoException e) {
            LOG.error("Cannot restore tasks queue", e);
        }
    }

    private Task getTask(String workflowId, String taskId) throws StatusException {
        final Task task;
        try {
            task = taskDao.get(taskId);
            if (task == null) {
                throw io.grpc.Status.NOT_FOUND.withDescription("Task not found").asException();
            }
        } catch (DaoException e) {
            LOG.error("Error while getting task from dao", e);
            throw Status.INTERNAL.withDescription("Something wrong with service").asException();
        }
        return task;
    }

    private void validateTask(TaskDesc taskDesc) throws StatusException {

        Set<String> inputSlots = taskDesc.operation().slots().stream()
            .filter(s -> s.direction() == Slot.Direction.INPUT)
            .map(Slot::name)
            .collect(Collectors.toSet());

        Set<String> outputSlots = taskDesc.operation().slots().stream()
            .filter(s -> s.direction() == Slot.Direction.OUTPUT)
            .map(Slot::name)
            .collect(Collectors.toSet());

        Set<String> intersection = new HashSet<>(inputSlots); // use the copy constructor
        intersection.retainAll(outputSlots);

        if (intersection.size() > 0) {
            throw Status.INVALID_ARGUMENT.withDescription("Some of slots are inputs and outputs at the same time")
                .asException();
        }

        if (!taskDesc.slotsToChannelsAssignments().keySet().containsAll(inputSlots)) {
            throw Status.INVALID_ARGUMENT.withDescription("Some of input slots are not mapped")
                .asException();
        }
        if (!taskDesc.slotsToChannelsAssignments().keySet().containsAll(outputSlots)) {
            throw Status.INVALID_ARGUMENT.withDescription("Some of output slots are not mapped")
                .asException();
        }

        Set<String> slots = new HashSet<>(inputSlots);
        slots.addAll(outputSlots);
        if (!slots.containsAll(taskDesc.slotsToChannelsAssignments().keySet())) {
            throw Status.INVALID_ARGUMENT.withDescription("Some of mappings are not presented in slots")
                .asException();
        }
    }
}
