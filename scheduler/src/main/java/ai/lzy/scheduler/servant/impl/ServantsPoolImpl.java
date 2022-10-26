package ai.lzy.scheduler.servant.impl;

import ai.lzy.model.ReturnCodes;
import ai.lzy.model.db.exceptions.DaoException;
import ai.lzy.model.operation.Operation;
import ai.lzy.scheduler.allocator.ServantsAllocator;
import ai.lzy.scheduler.configs.ServantEventProcessorConfig;
import ai.lzy.scheduler.configs.ServiceConfig;
import ai.lzy.scheduler.db.ServantDao;
import ai.lzy.scheduler.db.ServantEventDao;
import ai.lzy.scheduler.db.TaskDao;
import ai.lzy.scheduler.servant.Servant;
import ai.lzy.scheduler.servant.ServantsPool;
import ai.lzy.scheduler.task.Task;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;

@Singleton
public class ServantsPoolImpl implements ServantsPool {
    private final ServiceConfig config;
    private final ServantEventProcessorConfig servantConfig;
    private final ServantDao dao;
    private final ServantsAllocator allocator;
    private final ServantEventDao events;
    private final TaskDao tasks;
    private final Queue<ServantEventProcessor> processors = new ConcurrentLinkedQueue<>();
    private final Map<String, Set<Waiter>> waiters = new ConcurrentHashMap<>();
    private final Map<String, Map<String, Servant>> aliveServants = new ConcurrentHashMap<>();
    private final Map<String, Map<String, Servant>> freeServantsByWorkflow = new ConcurrentHashMap<>();
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
    public CompletableFuture<Servant> waitForFree(String userId, String workflowName,
                                                  Operation.Requirements requirements)
    {
        final CompletableFuture<Servant> future = new CompletableFuture<>();
        if (stopping.get()) {
            return null;
        }
        Servant free = tryToAcquire(workflowName, requirements);
        if (free != null) {
            future.complete(free);
            return future;
        }
        Servant servant = null;
        synchronized (this) {
            try {
                if (countAlive(workflowName, requirements) < limit(requirements)) {
                    servant = dao.create(userId, workflowName, requirements);
                }
            } catch (DaoException e) {
                LOG.error("Cannot count servants", e);
            }
        }
        if (servant != null) {
            initProcessor(servant);
            try {
                dao.acquireForTask(workflowName, servant.id());
            } catch (DaoException | ServantDao.AcquireException e) {
                LOG.error("Error while acquiring new servant", e);
                throw new RuntimeException(e);
            }
            future.complete(servant);
            return future;
        }

        waiters.computeIfAbsent(workflowName, t -> ConcurrentHashMap.newKeySet())
            .add(new Waiter(requirements, future));
        return future;
    }

    @Override
    public void shutdown() {
        processors.forEach(ServantEventProcessor::shutdown);
    }

    @Override
    public void waitForShutdown() throws InterruptedException {
        for (var processor: processors) {
            processor.shutdown();
            processor.join();
        }
        processors.clear();
    }

    private int limit(Operation.Requirements requirements) {
        return config.getProvisioningLimits().getOrDefault(requirements.poolLabel(),
            config.getDefaultProvisioningLimit());
    }

    private void restore() {
        final List<Servant> acquired;
        final List<Servant> free;
        try {
            acquired = dao.getAllAcquired();
            free = dao.getAllFree();
        } catch (DaoException e) {
            LOG.error("Cannot get acquired servants from dao", e);
            throw new RuntimeException(e);
        }
        for (Servant servant: acquired) {
            LOG.error("Cannot restart servant <{}> from workflow <{}> because it is in invalid state",
                servant.id(), servant.workflowName());
            try {
                dao.invalidate(servant, "Servant is in invalid state after restart");
            } catch (DaoException e) {
                LOG.error("Error while invalidating servant", e);
            }
            if (servant.taskId() != null) {
                try {
                    final Task task = tasks.get(servant.taskId());
                    if (task != null) {
                        task.notifyExecutionCompleted(ReturnCodes.INTERNAL_ERROR.getRc(),
                            "Failed because of servant wrong state");
                    }
                } catch (DaoException e) {
                    LOG.error("Error while invalidating servant", e);
                }
            }
            try {
                allocator.free(servant.workflowName(), servant.id());
            } catch (Exception e) {
                LOG.error("""
                    Cannot destroy servant <{}> from workflow <{}> with url <{}>, going to next servant.
                    PLEASE DESTROY THIS SERVANT FOR YOURSELF""", servant.id(), servant.workflowName(),
                    servant.servantURL(), e);
            }
        }
        for (Servant servant: free) {
            boolean isFree;
            try {
                dao.acquireForTask(servant.workflowName(), servant.id());
                isFree = true;
            } catch (ServantDao.AcquireException e) {
                LOG.debug("Servant {} is not free", servant.id());
                isFree = false;
            } catch (DaoException e) {
                LOG.error("Cannot restore servant {}", servant.id(), e);
                continue;
            }
            if (isFree) {
                free(servant.workflowName(), servant.id());
            }
            initProcessor(servant);
        }
    }

    @Nullable
    private synchronized Servant tryToAcquire(String workflowName, Operation.Requirements provisioning) {
        var map = freeServantsByWorkflow
            .computeIfAbsent(workflowName, t -> new ConcurrentHashMap<>());
        for (var servant: map.values()) {
            if (servant.requirements().equals(provisioning)) {
                try {
                    dao.acquireForTask(servant.workflowName(), servant.id());
                    map.remove(servant.id());
                } catch (DaoException e) {
                    throw new RuntimeException("Cannot acquire servant", e);
                } catch (ServantDao.AcquireException e) {
                    map.remove(servant.id());
                    continue;
                }
                return servant;
            }
        }
        return null;
    }

    private synchronized void free(String workflowName, String servantId) {
        LOG.debug("Servant {} from workflow {} freed", servantId, workflowName);
        final Servant servant;
        try {
            dao.freeFromTask(workflowName, servantId);
            servant = dao.get(workflowName, servantId);
        } catch (DaoException e) {
            throw new RuntimeException("Cannot free servant", e);
        }
        if (servant == null) {
            return;
        }
        final var set = waiters.computeIfAbsent(workflowName, t -> new HashSet<>());
        for (var waiter: set) {
            if (servant.requirements().equals(waiter.requirements)) {
                try {
                    dao.acquireForTask(workflowName, servantId);
                } catch (DaoException e) {
                    LOG.error("Cannot acquire servant", e);
                    continue;
                } catch (ServantDao.AcquireException e) {
                    continue;
                }
                set.remove(waiter);
                waiter.future.complete(servant);
                return;
            }
        }
        freeServantsByWorkflow.computeIfAbsent(workflowName, t -> new ConcurrentHashMap<>())
            .put(servantId, servant);
    }

    private synchronized void servantDestroyed(String workflowName, String servantId) {
        var servant = aliveServants.get(workflowName).remove(servantId);
        var set = waiters.get(workflowName);
        if (set == null) {
            return;
        }
        for (var waiter: set) {
            if (servant.requirements().equals(waiter.requirements)) {
                // TODO(artolord) make more fair scheduling
                try {
                    if (countAlive(workflowName, waiter.requirements) < limit(waiter.requirements)) {
                        set.remove(waiter);
                        var newServant = dao.create(servant.userId(), workflowName, waiter.requirements);
                        initProcessor(newServant);
                        dao.acquireForTask(workflowName, newServant.id());
                        waiter.future.complete(newServant);
                    }
                } catch (DaoException | ServantDao.AcquireException e) {
                    LOG.error("Error while creating servant", e);
                }
            }
        }
    }

    private int countAlive(String workflowId, Operation.Requirements requirements) {
        int count = 0;
        var map = aliveServants.computeIfAbsent(workflowId, t -> new ConcurrentHashMap<>());
        for (var servant: map.values()) {
            if (servant.requirements().equals(requirements)) {
                count++;
            }
        }
        return count;
    }

    private record Waiter(
        Operation.Requirements requirements,
        CompletableFuture<Servant> future
    ) {}

    private void initProcessor(Servant servant) {
        aliveServants.computeIfAbsent(servant.workflowName(),
            t -> new ConcurrentHashMap<>()).put(servant.id(), servant);
        var processor = new ServantEventProcessor(servant.workflowName(), servant.id(),
            servantConfig, allocator, tasks, events, dao, queueManager,
            this::free, this::servantDestroyed);
        processors.add(processor);
        processor.start();
    }
}
