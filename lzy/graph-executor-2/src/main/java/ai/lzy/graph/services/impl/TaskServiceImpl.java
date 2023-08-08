package ai.lzy.graph.services.impl;

import ai.lzy.graph.LGE;
import ai.lzy.graph.config.ServiceConfig;
import ai.lzy.graph.db.TaskDao;
import ai.lzy.graph.db.impl.GraphExecutorDataSource;
import ai.lzy.graph.model.TaskState;
import ai.lzy.graph.services.TaskService;
import ai.lzy.graph.services.WorkerService;
import ai.lzy.longrunning.Operation;
import ai.lzy.longrunning.OperationsExecutor;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.TransactionHandle;
import jakarta.annotation.Nullable;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static ai.lzy.model.db.DbHelper.withRetries;

@Singleton
public class TaskServiceImpl implements TaskService {
    private static final Logger LOG = LogManager.getLogger(TaskServiceImpl.class);

    private final ServiceConfig config;
    private final TaskDao taskDao;
    private final GraphExecutorDataSource storage;
    private final WorkerService workerService;
    private final OperationDao operationDao;
    private final OperationsExecutor operationsExecutor;

    private final Lock modifyReadyTasksLock = new ReentrantLock();
    private final Condition readyTasksModified = modifyReadyTasksLock.newCondition();

    private final Map<String, TaskState> readyTasks = new ConcurrentHashMap<>();
    private final Map<String, TaskState> waitingTasks = new ConcurrentHashMap<>();
    private final Map<String, TaskState> runningTask = new ConcurrentHashMap<>();

    private final Map<String, Integer> limitByUser = new ConcurrentHashMap<>();
    private final Map<String, Integer> limitByWorkflow = new ConcurrentHashMap<>();

    private final ScheduledExecutorService schedulerExecutor = Executors.newSingleThreadScheduledExecutor();

    private volatile Consumer<TaskState> taskOnStatusChanged = ts -> LOG.error("Handler on task status not set.");

    private final AtomicBoolean terminate = new AtomicBoolean(false);
    private final AtomicBoolean terminated = new AtomicBoolean(false);

    @Inject
    public TaskServiceImpl(ServiceConfig config, TaskDao taskDao, GraphExecutorDataSource storage,
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

        restoreTasks(config.getInstanceId());
        schedulerExecutor.scheduleWithFixedDelay(this::runReadyTasks, 1, 1, TimeUnit.SECONDS);
    }

    @Override
    public void init(Consumer<TaskState> taskOnStatusChanged) {
        this.taskOnStatusChanged = taskOnStatusChanged;
    }

    @Override
    public void addTasks(List<TaskState> tasks) {
        tasks.forEach(task -> {
            if (task.tasksDependedOn().isEmpty()) {
                modifyReadyTasksLock.lock();
                try {
                    if (readyTasks.put(task.id(), task) == null) {
                        readyTasksModified.signal();
                    }
                } finally {
                    modifyReadyTasksLock.unlock();
                }
            } else {
                waitingTasks.put(task.id(), task);
            }
        });
    }

    @Override
    @Nullable
    public LGE.TaskExecutionStatus getTaskStatus(String taskId) {
        var task = readyTasks.get(taskId);
        if (task == null) {
            task = runningTask.get(taskId);
        }
        if (task == null) {
            task = waitingTasks.get(taskId);
        }
        return task != null ? task.toProtoStatus() : null;
    }

    @PreDestroy
    private void shutdown() {
        if (terminate.compareAndSet(false, true)) {
            schedulerExecutor.shutdown();
            while (!terminated.get()) {
                LockSupport.parkNanos(10_000);
            }
        }
    }

    private void runReadyTasks() {
        if (terminate.get()) {
            terminated.set(true);
            return;
        }

        modifyReadyTasksLock.lock();
        try {
            if (readyTasks.isEmpty()) {
                readyTasksModified.await();
            }
        } catch (InterruptedException e) {
            if (terminate.get()) {
                terminated.set(true);
                return;
            }
            LOG.error("Unexpected interrupt", e);
            return;
        } finally {
            modifyReadyTasksLock.unlock();
        }

        var tasks = new ArrayList<>(readyTasks.values());

        // sort by dependant tasks
        tasks.sort(Comparator.comparingInt(task -> task.tasksDependedFrom().size()));

        int fromIdx = 0;
        var candidates = new ArrayList<TaskState>();

        while (fromIdx < tasks.size()) {
            int deps = tasks.get(fromIdx).tasksDependedFrom().size();

            for (int i = fromIdx; i < tasks.size(); i++) {
                var task = tasks.get(i);
                if (task.tasksDependedOn().size() == deps) {
                    candidates.add(task);
                } else {
                    fromIdx = i;
                    break;
                }
            }

            candidates.removeIf(task -> {
                var limits = config.getExecLimits();
                var drop = (limitByUser.getOrDefault(task.userId(), 0) >= limits.getMaxUserRunningTasks()) ||
                    (limitByWorkflow.getOrDefault(task.workflowName(), 0) >= limits.getMaxWorkflowRunningTasks());
                if (drop) {
                    LOG.debug("Cannot run task {}, limit exceeded", task.id());
                }
                return drop;
            });

            if (!candidates.isEmpty()) {
                break;
            }
        }

        if (candidates.isEmpty()) {
            LOG.debug("Nothing to run, limits exceeded");
            return;
        }

        for (var candidate : candidates) {
            if (terminate.get()) {
                terminated.set(true);
                return;
            }

            // TODO: existing op

            var idk = "%s/%s/%s".formatted(candidate.userId(), candidate.executionId(), candidate.id());

            var op = Operation.create(
                candidate.userId(),
                "Execute task '%s', graph '%s'".formatted(candidate.id(), candidate.graphId()),
                /* timeout */ null,
                new Operation.IdempotencyKey(idk, idk),
                /* meta */ null);

            final var task = candidate.toWaitAllocation(op.id());

            var executeTaskAction = new ExecuteTaskAction(op.id(), task, "", storage, operationDao, operationsExecutor,
                taskDao, workerService, this::completeTask);

            try {
                withRetries(LOG, () -> {
                    try (var tx = TransactionHandle.create(storage)) {
                        operationDao.create(op, tx);
                        taskDao.updateTask(task, tx);
                        tx.commit();
                    }
                });
            } catch (Exception e) {
                LOG.error("Couldn't update task {}, trying again.", task.id());
                break;
            }

            LOG.info("Created task operation {} for task {}", op.id(), task.id());

            readyTasks.remove(task.id());
            runningTask.put(task.id(), task);

            taskOnStatusChanged.accept(task);
            operationsExecutor.startNew(executeTaskAction);
        }
    }

    private void completeTask(TaskState task) {
        limitByUser.merge(task.userId(), -1, Integer::sum);
        limitByWorkflow.merge(task.executionId(), -1, Integer::sum);

        runningTask.remove(task.id());

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

    private void restoreTasks(String instanceId) {
        try {
            withRetries(LOG, () -> {
                var taskList = taskDao.loadActiveTasks(instanceId);

                var failedGraphs = taskList.stream()
                    .filter(task -> task.status() == TaskState.Status.FAILED)
                    .map(TaskState::graphId)
                    .distinct()
                    .toList();

                if (!failedGraphs.isEmpty()) {
                    LOG.error("Found failed tasks for not failed graphs {}", failedGraphs);
                    throw new RuntimeException();
                }

                var completedTasks = taskList.stream()
                    .filter(task -> task.status() == TaskState.Status.COMPLETED)
                    .toList();
                var waitingTasksMap = taskList.stream()
                    .filter(task -> task.status() == TaskState.Status.WAITING)
                    .collect(Collectors.toMap(TaskState::id, task -> task));

                for (var task: completedTasks) {
                    for (var depTaskId: task.tasksDependedFrom()) {
                        var depTask = waitingTasksMap.get(depTaskId);
                        depTask.tasksDependedOn().remove(task.id());
                    }
                }

                waitingTasksMap.values().stream()
                    .filter(task -> task.tasksDependedOn().isEmpty())
                    .forEach(task -> readyTasks.put(task.id(), task));

                waitingTasksMap.values().stream()
                    .filter(task -> !task.tasksDependedOn().isEmpty())
                    .forEach(task -> waitingTasks.put(task.id(), task));

                var statuses = Set.of(TaskState.Status.WAITING_ALLOCATION,
                    TaskState.Status.ALLOCATING, TaskState.Status.EXECUTING);
                var executingTasks = taskList.stream()
                    .filter(task -> statuses.contains(task.status()))
                    .toList();

                for (var task: executingTasks) {
                    LOG.info("Restore execute action for task {}", task.id());
                    ExecuteTaskAction executeTaskAction = new ExecuteTaskAction(task.executingState().opId(), task, "",
                        storage, operationDao, operationsExecutor, taskDao, workerService, this::completeTask);
                    operationsExecutor.startNew(executeTaskAction);
                }
            });
        } catch (Exception e) {
            LOG.error("Cannot restore tasks for instance {}", instanceId);
            throw new RuntimeException(e);
        }
    }
}
