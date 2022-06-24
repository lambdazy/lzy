package ai.lzy.scheduler.servant.impl;

import ai.lzy.scheduler.configs.ServiceConfig;
import ai.lzy.scheduler.db.DaoException;
import ai.lzy.scheduler.db.ServantDao;
import ai.lzy.scheduler.db.TaskDao;
import ai.lzy.scheduler.models.TaskDesc;
import ai.lzy.scheduler.models.TaskState;
import ai.lzy.scheduler.servant.Scheduler;
import ai.lzy.scheduler.servant.Servant;
import ai.lzy.scheduler.servant.ServantsPool;
import ai.lzy.scheduler.task.Task;
import io.grpc.Status;
import io.grpc.StatusException;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@Singleton
public class SchedulerImpl extends Thread implements Scheduler {
    private final ServantDao dao;
    private final TaskDao taskDao;
    private final ServantsPool pool;

    private static final Logger LOG = LogManager.getLogger(SchedulerImpl.class);
    private final BlockingQueue<Task> tasks = new LinkedBlockingQueue<>();
    private final AtomicBoolean stopping = new AtomicBoolean(false);

    @Inject
    public SchedulerImpl(ServantDao dao, TaskDao taskDao, ServantsPool pool) {
        this.dao = dao;
        this.taskDao = taskDao;
        this.pool = pool;
        restore();
    }

    @Override
    public Task execute(String workflowId, TaskDesc taskDesc) throws StatusException {
        // TODO(artolord) add task validation
        if (stopping.get()) {
            throw io.grpc.Status.UNAVAILABLE.withDescription("Service is stopping. Please try again").asException();
        }
        try {
            final Task task = taskDao.create(workflowId, taskDesc);
            tasks.add(task);
            return task;
        } catch (DaoException e) {
            throw io.grpc.Status.INTERNAL.withDescription("Cannot create task").asException();
        }
    }

    @Override
    public void signal(String workflowId, String taskId, int signalNumber, String issue) throws StatusException {
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

        servant.signal(signalNumber);
    }

    @Override
    public Task status(String workflowId, String taskId) throws StatusException {
        if (stopping.get()) {
            throw io.grpc.Status.UNAVAILABLE.withDescription("Service is stopping. Please try again").asException();
        }

        return getTask(workflowId, taskId);
    }

    @Override
    public void gracefulStop() {
        stopping.set(true);
        this.interrupt();
        pool.shutdown();
    }

    @Override
    public void run() {
        while (!stopping.get()) {
            final Task task;
            try {
                task = tasks.take();
            } catch (InterruptedException e) {
                LOG.debug("Thread interrupted", e);
                continue;
            }

            Servant servant;
            while (true) {
                try {
                    servant = pool.waitForFree(task.workflowId(), task.description().zygote().provisioning());
                    break;
                } catch (InterruptedException e) {
                    LOG.debug("Thread interrupted", e);
                    if (stopping.get()) {
                        return;
                    }
                }
            }

            if (servant == null) {
                LOG.info("Pool is stopping. Stopping scheduler");
                return;
            }

            servant.setTask(task);
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
            task = taskDao.get(workflowId, taskId);
            if (task == null) {
                throw io.grpc.Status.NOT_FOUND.withDescription("Task not found").asException();
            }
        } catch (DaoException e) {
            LOG.error("Error while getting task from dao", e);
            throw Status.INTERNAL.withDescription("Something wrong with service").asException();
        }
        return task;
    }
}
