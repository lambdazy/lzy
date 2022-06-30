package ai.lzy.scheduler.servant.impl;

import ai.lzy.model.ReturnCodes;
import ai.lzy.model.graph.Provisioning;
import ai.lzy.scheduler.allocator.ServantsAllocator;
import ai.lzy.scheduler.configs.ServantEventProcessorConfig;
import ai.lzy.scheduler.configs.ServiceConfig;
import ai.lzy.scheduler.db.DaoException;
import ai.lzy.scheduler.db.ServantDao;
import ai.lzy.scheduler.db.ServantEventDao;
import ai.lzy.scheduler.db.TaskDao;
import ai.lzy.scheduler.models.ServantState;
import ai.lzy.scheduler.servant.Servant;
import ai.lzy.scheduler.servant.ServantsPool;
import ai.lzy.scheduler.task.Task;
import jakarta.inject.Singleton;
import java.util.Comparator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@Singleton
public class ServantsPoolImpl extends Thread implements ServantsPool {
    private final ServiceConfig config;
    private final ServantEventProcessorConfig servantConfig;
    private final ServantDao dao;
    private final ServantsAllocator allocator;
    private final ServantEventDao events;
    private final TaskDao tasks;
    private final Queue<ServantEventProcessor> processors = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean stopping = new AtomicBoolean(false);
    private final EventQueueManager queueManager;
    private static final Logger LOG = LogManager.getLogger(ServantsPoolImpl.class);

    public ServantsPoolImpl(ServiceConfig config, ServantEventProcessorConfig servantConfig,
                            ServantDao dao, ServantsAllocator allocator, ServantEventDao events,
                            TaskDao tasks, EventQueueManager queueManager) {
        this.config = config;
        this.servantConfig = servantConfig;
        this.dao = dao;
        this.allocator = allocator;
        this.events = events;
        this.tasks = tasks;
        this.queueManager = queueManager;
        restore();
    }

    @Nullable
    @Override
    public Servant waitForFree(String workflowId, Provisioning provisioning) throws InterruptedException {
        if (stopping.get()) {
            return null;
        }
        Servant free = null;
        try {
            free = dao.acquireForTask(workflowId, provisioning, ServantState.Status.RUNNING,
                    ServantState.Status.IDLE);
        } catch (DaoException e) {
            LOG.error("Cannot acquire servant", e);
        }
        if (free != null) {
            return free;
        }
        Servant servant = null;
        synchronized (this) {
            try {
                if (dao.countAlive(workflowId, provisioning) < limit(provisioning)) {
                    servant = dao.create(workflowId, provisioning);
                }
            } catch (DaoException e) {
                LOG.error("Cannot count servants");
            }
        }
        if (servant != null) {
            var processor = new ServantEventProcessor(workflowId, servant.id(), servantConfig,
                allocator, tasks, events, dao, queueManager,
                (workflow, servantId) -> {
                    synchronized (this) {
                        this.notifyAll();
                    }
            });
            processor.start();
            processors.add(processor);
        }

        Servant allocatedServant = null;
        while (allocatedServant == null) {
            if (stopping.get()) {
                return null;
            }
            synchronized (this) {
                this.wait();
            }
            try {
                allocatedServant = dao.acquireForTask(workflowId, provisioning,
                    ServantState.Status.RUNNING, ServantState.Status.IDLE);
            } catch (DaoException e) {
                LOG.error("Cannot acquire servant while waiting", e);
            }
        }

        return allocatedServant;
    }

    @Override
    public void shutdown() {
        processors.forEach(ServantEventProcessor::shutdown);
    }

    @Override
    public void waitForShutdown() throws InterruptedException {

    }

    @Override
    public void destroy() {

    }

    private int limit(Provisioning provisioning) {
        return provisioning.tags()
            .map(s -> config.provisioningLimits().getOrDefault(s.tag(), 0))
            .min(Comparator.comparingInt(t -> t))
            .orElse(config.defaultProvisioningLimit());
    }

    private synchronized void restore() {
        final List<Servant> acquired, free;
        try {
            acquired = dao.getAllAcquired();
            free = dao.getAllFree();
        } catch (DaoException e) {
            LOG.error("Cannot get acquired servants from dao", e);
            throw new RuntimeException(e);
        }
        for (Servant servant: acquired) {
            LOG.error("Cannot restart servant <{}> from workflow <{}> because it is in invalid state",
                servant.id(), servant.workflowId());
            try {
                dao.invalidate(servant, "Servant is in invalid state after restart");
            } catch (DaoException e) {
                LOG.error("Error while invalidating servant", e);
            }
            if (servant.taskId() != null) {
                try {
                    final Task task = tasks.get(servant.workflowId(), servant.taskId());
                    if (task != null) {
                        task.notifyExecutionCompleted(ReturnCodes.INTERNAL.getRc(),
                            "Failed because of servant wrong state");
                    }
                } catch (DaoException e) {
                    LOG.error("Error while invalidating servant", e);
                }
            }
            try {
                allocator.destroy(servant.workflowId(), servant.id());
            } catch (Exception e) {
                LOG.error("""
                    Cannot destroy servant <{}> from workflow <{}> with url <{}>, going to next servant.
                    PLEASE DESTROY THIS SERVANT FOR YOURSELF""", servant.id(), servant.workflowId(),
                    servant.servantURL(), e);
            }
        }
        for (Servant servant: free) {
            var processor = new ServantEventProcessor(servant.workflowId(), servant.id(),
                servantConfig, allocator, tasks, events, dao, queueManager,
                (workflow, servantId) -> {
                    synchronized (this) {
                        this.notifyAll();
                    }
                });
            processor.start();
            processors.add(processor);
        }
    }
}
