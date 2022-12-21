package ai.lzy.scheduler.worker.impl;

import ai.lzy.model.ReturnCodes;
import ai.lzy.model.db.exceptions.DaoException;
import ai.lzy.model.operation.Operation;
import ai.lzy.scheduler.allocator.WorkersAllocator;
import ai.lzy.scheduler.configs.ServiceConfig;
import ai.lzy.scheduler.configs.WorkerEventProcessorConfig;
import ai.lzy.scheduler.db.TaskDao;
import ai.lzy.scheduler.db.WorkerDao;
import ai.lzy.scheduler.db.WorkerEventDao;
import ai.lzy.scheduler.task.Task;
import ai.lzy.scheduler.worker.Worker;
import ai.lzy.scheduler.worker.WorkersPool;
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
public class WorkersPoolImpl implements WorkersPool {
    private final ServiceConfig config;
    private final WorkerEventProcessorConfig workerConfig;
    private final WorkerDao dao;
    private final WorkersAllocator allocator;
    private final WorkerEventDao events;
    private final TaskDao tasks;
    private final Queue<WorkerEventProcessor> processors = new ConcurrentLinkedQueue<>();
    private final Map<String, Set<Waiter>> waiters = new ConcurrentHashMap<>();
    private final Map<String, Map<String, Worker>> aliveWorkers = new ConcurrentHashMap<>();
    private final Map<String, Map<String, Worker>> freeWorkersByWorkflow = new ConcurrentHashMap<>();
    private final AtomicBoolean stopping = new AtomicBoolean(false);
    private final EventQueueManager queueManager;
    private static final Logger LOG = LogManager.getLogger(WorkersPoolImpl.class);

    public WorkersPoolImpl(ServiceConfig config, WorkerEventProcessorConfig workerConfig,
                           WorkerDao dao, WorkersAllocator allocator, WorkerEventDao events,
                           TaskDao tasks, EventQueueManager queueManager)
    {
        this.config = config;
        this.workerConfig = workerConfig;
        this.dao = dao;
        this.allocator = allocator;
        this.events = events;
        this.tasks = tasks;
        this.queueManager = queueManager;
        restore();
    }

    @Nullable
    @Override
    public CompletableFuture<Worker> waitForFree(String userId, String workflowName,
                                                 Operation.Requirements requirements)
    {
        final CompletableFuture<Worker> future = new CompletableFuture<>();
        if (stopping.get()) {
            return null;
        }
        Worker free = tryToAcquire(workflowName, requirements);
        if (free != null) {
            future.complete(free);
            return future;
        }
        Worker worker = null;
        synchronized (this) {
            try {
                if (countAlive(workflowName, requirements) < limit(requirements)) {
                    worker = dao.create(userId, workflowName, requirements);
                }
            } catch (DaoException e) {
                LOG.error("Cannot count worker", e);
            }
        }
        if (worker != null) {
            initProcessor(worker);
            try {
                dao.acquireForTask(workflowName, worker.id());
            } catch (DaoException | WorkerDao.AcquireException e) {
                LOG.error("Error while acquiring new worker", e);
                throw new RuntimeException(e);
            }
            future.complete(worker);
            return future;
        }

        waiters.computeIfAbsent(workflowName, t -> ConcurrentHashMap.newKeySet())
            .add(new Waiter(requirements, future));
        return future;
    }

    @Override
    public void shutdown() {
        processors.forEach(WorkerEventProcessor::shutdown);
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
        final List<Worker> acquired;
        final List<Worker> free;
        try {
            acquired = dao.getAllAcquired();
            free = dao.getAllFree();
        } catch (DaoException e) {
            LOG.error("Cannot get acquired workers from dao", e);
            throw new RuntimeException(e);
        }
        for (Worker worker : acquired) {
            LOG.error("Cannot restart worker <{}> from workflow <{}> because it is in invalid state",
                worker.id(), worker.workflowName());
            try {
                dao.invalidate(worker, "Worker is in invalid state after restart");
            } catch (DaoException e) {
                LOG.error("Error while invalidating worker", e);
            }
            if (worker.taskId() != null) {
                try {
                    final Task task = tasks.get(worker.taskId());
                    if (task != null) {
                        task.notifyExecutionCompleted(ReturnCodes.INTERNAL_ERROR.getRc(),
                            "Failed because of worker wrong state");
                    }
                } catch (DaoException e) {
                    LOG.error("Error while invalidating worker", e);
                }
            }
            try {
                allocator.free(worker.workflowName(), worker.id());
            } catch (Exception e) {
                LOG.error("""
                    Cannot destroy worker <{}> from workflow <{}> with url <{}>, going to next worker.
                    PLEASE DESTROY THIS WORKER FOR YOURSELF""", worker.id(), worker.workflowName(),
                    worker.workerURL(), e);
            }
        }
        for (Worker worker : free) {
            boolean isFree;
            try {
                dao.acquireForTask(worker.workflowName(), worker.id());
                isFree = true;
            } catch (WorkerDao.AcquireException e) {
                LOG.debug("Worker {} is not free", worker.id());
                isFree = false;
            } catch (DaoException e) {
                LOG.error("Cannot restore worker {}", worker.id(), e);
                continue;
            }
            if (isFree) {
                free(worker.workflowName(), worker.id());
            }
            initProcessor(worker);
        }
    }

    @Nullable
    private synchronized Worker tryToAcquire(String workflowName, Operation.Requirements provisioning) {
        var map = freeWorkersByWorkflow
            .computeIfAbsent(workflowName, t -> new ConcurrentHashMap<>());
        for (var worker: map.values()) {
            if (worker.requirements().equals(provisioning)) {
                try {
                    dao.acquireForTask(worker.workflowName(), worker.id());
                    map.remove(worker.id());
                } catch (DaoException e) {
                    throw new RuntimeException("Cannot acquire worker", e);
                } catch (WorkerDao.AcquireException e) {
                    map.remove(worker.id());
                    continue;
                }
                return worker;
            }
        }
        return null;
    }

    private synchronized void free(String workflowName, String workerId) {
        LOG.debug("Worker {} from workflow {} freed", workerId, workflowName);
        final Worker worker;
        try {
            dao.freeFromTask(workflowName, workerId);
            worker = dao.get(workflowName, workerId);
        } catch (DaoException e) {
            throw new RuntimeException("Cannot free worker", e);
        }
        if (worker == null) {
            return;
        }
        final var set = waiters.computeIfAbsent(workflowName, t -> new HashSet<>());
        for (var waiter: set) {
            if (worker.requirements().equals(waiter.requirements)) {
                try {
                    dao.acquireForTask(workflowName, workerId);
                } catch (DaoException e) {
                    LOG.error("Cannot acquire worker", e);
                    continue;
                } catch (WorkerDao.AcquireException e) {
                    continue;
                }
                set.remove(waiter);
                waiter.future.complete(worker);
                return;
            }
        }
        freeWorkersByWorkflow.computeIfAbsent(workflowName, t -> new ConcurrentHashMap<>())
            .put(workerId, worker);
    }

    private synchronized void workerDestroyed(String workflowName, String workerId) {
        var worker = aliveWorkers.get(workflowName).remove(workerId);
        var set = waiters.get(workflowName);
        if (set == null) {
            return;
        }
        for (var waiter: set) {
            if (worker.requirements().equals(waiter.requirements)) {
                // TODO(artolord) make more fair scheduling
                try {
                    if (countAlive(workflowName, waiter.requirements) < limit(waiter.requirements)) {
                        set.remove(waiter);
                        var newWorker = dao.create(worker.userId(), workflowName, waiter.requirements);
                        initProcessor(newWorker);
                        dao.acquireForTask(workflowName, newWorker.id());
                        waiter.future.complete(newWorker);
                    }
                } catch (DaoException | WorkerDao.AcquireException e) {
                    LOG.error("Error while creating worker", e);
                }
            }
        }
    }

    private int countAlive(String workflowId, Operation.Requirements requirements) {
        int count = 0;
        var map = aliveWorkers.computeIfAbsent(workflowId, t -> new ConcurrentHashMap<>());
        for (var worker: map.values()) {
            if (worker.requirements().equals(requirements)) {
                count++;
            }
        }
        return count;
    }

    private record Waiter(
        Operation.Requirements requirements,
        CompletableFuture<Worker> future
    ) {}

    private void initProcessor(Worker worker) {
        aliveWorkers.computeIfAbsent(worker.workflowName(),
            t -> new ConcurrentHashMap<>()).put(worker.id(), worker);
        var processor = new WorkerEventProcessor(worker.workflowName(), worker.id(),
            workerConfig, allocator, tasks, events, dao, queueManager,
            this::free, this::workerDestroyed);
        processors.add(processor);
        processor.start();
    }
}
