package ai.lzy.longrunning.task;

import ai.lzy.longrunning.OperationsExecutor;
import ai.lzy.longrunning.task.dao.OperationTaskDao;
import ai.lzy.model.db.DbHelper;
import ai.lzy.model.db.Storage;
import ai.lzy.model.db.TransactionHandle;
import jakarta.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class OperationTaskExecutor {

    private static final Logger LOG = LogManager.getLogger(OperationTaskExecutor.class);

    private final OperationTaskDao opTaskDao;
    private final OperationsExecutor operationsExecutor;
    private final OperationTaskResolver resolver;
    private final ScheduledExecutorService scheduler;
    private final Duration initialDelay;
    private final Duration executionDelay;
    private final Storage storage;
    private final TaskMetricsProvider metricsProvider;
    private final String instanceId;
    private final Duration leaseDuration;
    private final int batchSize;

    private volatile boolean started = false;
    private volatile boolean disabled = false;

    public OperationTaskExecutor(OperationTaskDao opTaskDao, OperationsExecutor operationsExecutor,
                                 OperationTaskResolver resolver, Duration initialDelay, Duration executionDelay,
                                 Storage storage, TaskMetricsProvider metricsProvider, String instanceId,
                                 Duration leaseDuration, int batchSize)
    {
        this.opTaskDao = opTaskDao;
        this.operationsExecutor = operationsExecutor;
        this.resolver = resolver;
        this.initialDelay = initialDelay;
        this.executionDelay = executionDelay;
        this.storage = storage;
        this.leaseDuration = leaseDuration;
        this.batchSize = batchSize;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(); //it's important to have only one thread
        this.metricsProvider = metricsProvider;
        this.instanceId = instanceId;
    }

    public void start() {
        if (started) {
            throw new IllegalStateException("Task executor has already started!");
        }
        started = true;
        restoreTasks();
        startMailLoop();
    }

    //todo backpressure - to not start new tasks if there are too many of them
    private ScheduledFuture<?> startMailLoop() {
        return scheduler.scheduleWithFixedDelay(() -> {
            try {
                var actions = new ArrayList<OpTaskAwareAction>();
                DbHelper.withRetries(LOG, () -> {
                    try (var tx = TransactionHandle.create(storage)) {
                        for (OperationTask operationTask : opTaskDao.lockPendingBatch(instanceId, leaseDuration,
                            batchSize, tx))
                        {
                            if (disabled) {
                                return;
                            }
                            var taskAwareAction = resolveTask(operationTask, tx);
                            if (taskAwareAction != null) {
                                actions.add(taskAwareAction);
                            }
                        }
                        tx.commit();
                    }
                });
                actions.forEach(operationsExecutor::startNew);
            } catch (Exception e) {
                LOG.error("Got exception while scheduling task", e);
                metricsProvider.schedulerErrors(instanceId).inc();
            }
        }, initialDelay.toMillis(), executionDelay.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Nullable
    private OpTaskAwareAction resolveTask(OperationTask operationTask, TransactionHandle tx) throws SQLException {
        var resolveResult = resolver.resolve(operationTask, tx);
        if (resolveResult.status() != OperationTaskResolver.Status.SUCCESS) {
            metricsProvider.schedulerResolveErrors(instanceId, resolveResult.status());
        }
        switch (resolveResult.status()) {
            case SUCCESS -> {
                var updatedTask = setStatus(operationTask, OperationTask.Status.RUNNING, tx);
                var action = resolveResult.action();
                assert action != null;
                action.setTask(updatedTask);
                return action;
            }
            case STALE -> {
                LOG.warn("Marking task {} as STALE", operationTask.id(), resolveResult.exception());
                setStatus(operationTask, OperationTask.Status.STALE, tx);
            }
            case BAD_STATE -> {
                LOG.error("Marking task {} as FAILED", operationTask.id(), resolveResult.exception());
                setStatus(operationTask, OperationTask.Status.FAILED, tx);
            }
            case UNKNOWN_TASK, RESOLVE_ERROR -> {
                LOG.warn("Couldn't resolve task {}", operationTask.id(), resolveResult.exception());
            }
        }
        return null;
    }

    private void restoreTasks() {
        try {
            var actionsToRun = new ArrayList<OpTaskAwareAction>();
            DbHelper.withRetries(LOG, () -> {
                try (var tx = TransactionHandle.create(storage)) {
                    var tasks = opTaskDao.recaptureOldTasks(instanceId, leaseDuration, tx);
                    for (OperationTask operationTask : tasks) {
                        var taskAwareAction = resolveTask(operationTask, tx);
                        if (taskAwareAction != null) {
                            actionsToRun.add(taskAwareAction);
                        }
                    }
                    tx.commit();
                }
            });
            actionsToRun.forEach(operationsExecutor::startNew);
        } catch (Exception e) {
            LOG.error("Got exception while restoring tasks", e);
            metricsProvider.schedulerErrors(instanceId).inc();
        }
    }

    private OperationTask setStatus(OperationTask operationTask, OperationTask.Status status, TransactionHandle tx)
        throws SQLException
    {
        return opTaskDao.update(operationTask.id(), OperationTask.Update.builder()
            .status(status)
            .build(), tx);
    }

    public void saveTask(OperationTask operationTask, @Nullable TransactionHandle tx) throws SQLException {
        opTaskDao.insert(operationTask, tx);
    }

    public ScheduledFuture<Boolean> startImmediately(OperationTask opTask) {
        //schedule runnable to start task immediately.
        //if task is already captured by main loop, this runnable will exit
        return scheduler.schedule(() -> {
            try {
                var action = DbHelper.withRetries(LOG, () -> {
                    try (var tx = TransactionHandle.create(storage)) {
                        var lockedTask = opTaskDao.tryLockTask(opTask.id(),
                            opTask.entityId(), instanceId, leaseDuration,
                            tx);
                        if (lockedTask == null) {
                            return null;
                        }
                        var taskAwareAction = resolveTask(lockedTask, tx);
                        tx.commit();
                        return taskAwareAction;
                    }
                });
                if (action == null) {
                    return false;
                }
                operationsExecutor.startNew(action);
                return true;
            } catch (Exception e) {
                LOG.error("Got exception while scheduling task", e);
                metricsProvider.schedulerErrors(instanceId).inc();
                return false;
            }
        }, 0, TimeUnit.MILLISECONDS);
    }

    public void shutdown() {
        disabled = true;
        scheduler.shutdown();
    }
}
