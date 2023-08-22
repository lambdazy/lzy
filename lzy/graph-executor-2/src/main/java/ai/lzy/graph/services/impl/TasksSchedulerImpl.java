package ai.lzy.graph.services.impl;

import ai.lzy.graph.LGE;
import ai.lzy.graph.config.ServiceConfig;
import ai.lzy.graph.db.TaskDao;
import ai.lzy.graph.db.impl.GraphExecutorDataSource;
import ai.lzy.graph.model.TaskState;
import ai.lzy.graph.services.TasksScheduler;
import ai.lzy.graph.services.WorkerService;
import ai.lzy.longrunning.Operation;
import ai.lzy.longrunning.OperationsExecutor;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.TransactionHandle;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static ai.lzy.model.db.DbHelper.withRetries;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

@Singleton
public class TasksSchedulerImpl implements TasksScheduler {
    private static final Logger LOG = LogManager.getLogger(TasksSchedulerImpl.class);

    private final ServiceConfig config;
    private final TaskDao taskDao;
    private final GraphExecutorDataSource storage;
    private final WorkerService workerService;
    private final OperationDao operationDao;
    private final OperationsExecutor operationsExecutor;

    private final Map<String, TaskState> readyTasks = new ConcurrentHashMap<>();
    private final Map<String, TaskState> waitingTasks = new ConcurrentHashMap<>();
    private final Map<String, TaskState> runningTask = new ConcurrentHashMap<>();

    private final Map<String, Integer> limitByUser = new ConcurrentHashMap<>();
    private final Map<String, Integer> limitByWorkflow = new ConcurrentHashMap<>();

    private final ScheduledExecutorService schedulerExecutor = Executors.newSingleThreadScheduledExecutor();

    private volatile Consumer<TaskState> taskOnStatusChanged = ts -> LOG.error("Handler on task status not set.");
    @Nullable
    private volatile ScheduledFuture<?> schedulerFuture = null;

    private final AtomicBoolean terminate = new AtomicBoolean(false);
    private final AtomicBoolean terminated = new AtomicBoolean(false);

    @Inject
    public TasksSchedulerImpl(ServiceConfig config, TaskDao taskDao, GraphExecutorDataSource storage,
                              WorkerService workerService,
                              @Named("GraphExecutorOperationDao") OperationDao operationDao,
                              @Named("GraphExecutorOperationsExecutor") OperationsExecutor operationsExecutor)
    {
        this.taskDao = taskDao;
        this.operationsExecutor = operationsExecutor;
        this.storage = storage;
        this.operationDao = operationDao;
        this.workerService = workerService;
        this.config = config;
    }

    @Override
    public void start(Consumer<TaskState> taskOnStatusChanged) {
        assert !terminate.get();
        this.taskOnStatusChanged = taskOnStatusChanged;
        if (schedulerFuture == null) {
            schedulerFuture = schedulerExecutor.scheduleWithFixedDelay(this::runReadyTasks, 1, 1, TimeUnit.SECONDS);
        }
    }

    @Override
    public void shutdown() {
        if (terminate.compareAndSet(false, true)) {
            schedulerExecutor.shutdown();
            while (!terminated.get()) {
                LockSupport.parkNanos(10_000);
            }
        }
    }

    @Override
    public void restoreGraphTasks(String graphId, Collection<TaskState> waiting, Collection<TaskState> running) {
        scheduleGraphTasks(graphId, waiting);

        for (var task : running) {
            assert task.graphId().equals(graphId);
            assert task.status().running();

            LOG.info("Restore execute action for graph {} task {}", graphId, task.id());
            var action = createExecuteTaskAction(task, requireNonNull(task.executingState()).opId());
            operationsExecutor.startNew(action);
        }
    }

    @Override
    public void scheduleGraphTasks(String graphId, Collection<TaskState> tasks) {
        for (var task : tasks) {
            assert task.graphId().equals(graphId);
            assert task.status() == TaskState.Status.WAITING;

            if (task.tasksDependedOn().isEmpty()) {
                readyTasks.put(task.id(), task);
            } else {
                waitingTasks.put(task.id(), task);
            }
        }
    }

    @Override
    @Nullable
    public LGE.TaskExecutionStatus getTaskStatus(String taskId) {
        var task = runningTask.get(taskId);
        if (task == null) {
            task = waitingTasks.get(taskId);
        }
        if (task == null) {
            task = readyTasks.get(taskId);
        }
        if (task == null) {
            try {
                task = withRetries(LOG, () -> taskDao.getTaskById(taskId));
            } catch (Exception e) {
                LOG.error("Cannot read task {} status: {}", taskId, e.getMessage());
            }
        }
        return task != null ? task.toProtoStatus() : null;
    }

    public void terminateGraphTasks(String graphId, String errorDescription) {
        BiConsumer<Map<String, TaskState>, Consumer<TaskState>> dropFailedGraphTasks = (tasks, fn) -> {
            for (var iter = tasks.values().iterator(); iter.hasNext(); ) {
                var task = iter.next();
                if (!task.graphId().equals(graphId)) {
                    continue;
                }
                task.fail("Graph failed: " + errorDescription);
                iter.remove();
                LOG.info("Terminate failed graph {}: drop {} task {}", graphId, task.status(), task.id());
                fn.accept(task);
            }
        };

        dropFailedGraphTasks.accept(readyTasks, task -> {});
        dropFailedGraphTasks.accept(waitingTasks, task -> {});

        dropFailedGraphTasks.accept(runningTask, task -> {
            switch (task.status()) {
                case WAITING_ALLOCATION, ALLOCATING, EXECUTING -> {
                    //operationDao.fail(task.executingState().opId(), "Abort execution: " + errorDescription, null);
                }
                case COMPLETED, FAILED -> { }
                default -> throw new RuntimeException("Unexpected task %s: %s".formatted(task.id(), task.status()));
            }
        });
    }

    private void runReadyTasks() {
        if (terminate.get()) {
            terminated.set(true);
            LOG.debug("Execution terminated");
            return;
        }

        if (readyTasks.isEmpty()) {
            return;
        }

        var tasks = readyTasks.values().stream()
            .filter(task -> {
                if (task.status() != TaskState.Status.WAITING) {
                    return false;
                }
                var limits = config.getExecLimits();
                if (limitByUser.getOrDefault(task.userId(), 0) >= limits.getMaxUserRunningTasks()) {
                    LOG.debug("Skip task {}, user limit exceeded", task.id());
                    return false;
                }
                if (limitByWorkflow.getOrDefault(task.workflowName(), 0) >= limits.getMaxWorkflowRunningTasks()) {
                    LOG.debug("Skip task {}, workflow limit exceeded", task.id());
                    return false;
                }
                return true;
            })
            .sorted(Comparator.comparingInt(task -> task.tasksDependedFrom().size()))
            .collect(Collectors.toList());

        if (tasks.isEmpty()) {
            LOG.debug("Nothing to run (out of {}), limits exceeded", readyTasks.size());
            return;
        }

        LOG.debug("Ready tasks: [{}]", tasks.stream().map(TaskState::toString).collect(joining(", ")));


        int deps = tasks.get(0).tasksDependedFrom().size();
        LOG.debug("Retain ready tasks with >= {} dependent tasks...", deps);
        tasks.removeIf(task -> task.tasksDependedFrom().size() >= deps);

        if (tasks.isEmpty()) {
            LOG.debug("Nothing to run (out of {})", readyTasks.size());
            return;
        }

        for (var task : tasks) {
            if (terminate.get()) {
                LOG.debug("Execution terminated");
                terminated.set(true);
                return;
            }

            if (task.status() != TaskState.Status.WAITING) {
                LOG.warn("Graph {}, task {}, status was changes from WAITING to {}, skip it",
                    task.graphId(), task.id(), task.status());
                readyTasks.remove(task.id());
                continue;
            }

            var idk = "%s/%s/%s".formatted(task.userId(), task.executionId(), task.id());

            var op = Operation.create(
                task.userId(),
                "Execute task '%s', graph '%s'".formatted(task.id(), task.graphId()),
                /* timeout */ null,
                new Operation.IdempotencyKey(idk, idk),
                /* meta */ null);

            task = task.toWaitAllocation(op.id());

            var executeTaskAction = createExecuteTaskAction(task, op.id());

            boolean taskUpdated;
            try {
                var finalTask = task;
                taskUpdated = withRetries(LOG, () -> {
                    try (var tx = TransactionHandle.create(storage)) {
                        operationDao.create(op, tx);
                        if (taskDao.updateTask(finalTask, TaskState.Status.WAITING, tx)) {
                            tx.commit();
                            return true;
                        }
                        return false;
                    }
                });
            } catch (Exception e) {
                LOG.error("Couldn't update task {}: {}. Try later.", task.id(), e.getMessage());
                // TODO: it may hangs
                continue;
            }

            if (taskUpdated) {
                LOG.info("Created task operation {} for task {}", op.id(), task.id());

                readyTasks.remove(task.id());
                runningTask.put(task.id(), task);

                taskOnStatusChanged.accept(task);
                operationsExecutor.startNew(executeTaskAction);
            } else {
                LOG.warn("Cannot start task {}, graph {}: unexpected task status", task.id(), task.graphId());
            }
        }
    }

    @Nonnull
    private ExecuteTaskAction createExecuteTaskAction(TaskState task, String execOpId) {
        return new ExecuteTaskAction(
            execOpId,
            task,
            "graphId: %s, taskId: %s, wf: %s, execId: %s, userId: %s"
                .formatted(task.graphId(), task.id(), task.workflowName(), task.executionId(), task.userId()),
            storage,
            operationDao,
            operationsExecutor,
            taskDao,
            workerService,
            this::finishTask);
    }

    private void finishTask(TaskState task) {
        assert task.status().finished();

        limitByUser.merge(task.userId(), -1, Integer::sum);
        limitByWorkflow.merge(task.executionId(), -1, Integer::sum);

        runningTask.remove(task.id());

        if (task.status() == TaskState.Status.FAILED) {
            LOG.error("Graph {}, task {} failed: {}", task.graphId(), task.id(), task.errorDescription());

            terminateGraphTasks(task.graphId(), "Task %s was failed with reason '%s'"
                .formatted(task.id(), task.errorDescription()));

            taskOnStatusChanged.accept(task);
            return;
        }

        for (var taskId : task.tasksDependedFrom()) {
            var waitingTask = waitingTasks.get(taskId);
            waitingTask.tasksDependedOn().remove(task.id());

            if (waitingTask.tasksDependedOn().isEmpty()) {
                waitingTasks.remove(taskId);
                readyTasks.put(waitingTask.id(), waitingTask);
            }
        }
        taskOnStatusChanged.accept(task);
    }
}
