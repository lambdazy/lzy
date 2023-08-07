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
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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

    private final PriorityQueue<TaskState> readyTasks = new PriorityQueue<>(
        Comparator.comparingInt(task -> task.tasksDependedFrom().size()));
    private final Map<String, TaskState> runningTask = new ConcurrentHashMap<>();
    private final Map<String, TaskState> waitingTasks = new ConcurrentHashMap<>();

    //private final Map<String, Integer> limitByUser = new ConcurrentHashMap<>();
    //private final Map<String, Integer> limitByWorkflow = new ConcurrentHashMap<>();

    private final ScheduledExecutorService workExecutor = Executors.newSingleThreadScheduledExecutor();

    private volatile Consumer<TaskState> taskOnStatusChanged = __ -> LOG.error("Handler on task status not set.");

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
        workExecutor.scheduleWithFixedDelay(this::run, 1, 1, TimeUnit.SECONDS);
    }

    @Override
    public void init(Consumer<TaskState> taskOnStatusChanged) {
        this.taskOnStatusChanged = taskOnStatusChanged;
    }

    @Override
    public void addTasks(List<TaskState> tasks) {
        tasks.forEach(task -> {
            if (task.tasksDependedOn().isEmpty()) {
                readyTasks.add(task);
            } else {
                waitingTasks.put(task.id(), task);
            }
        });
    }

    @Override
    @Nullable
    public LGE.TaskExecutionStatus getTaskStatus(String taskId) {
        // TODO: keep tasks states in memory
        try {
            var task = withRetries(LOG, () -> taskDao.getTaskById(taskId));
            return task != null ? task.toProtoStatus() : null;
        } catch (Exception e) {
            LOG.error("Cannot load task {}: {}", taskId, e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private void run() {
        var candidate = readyTasks.peek();
        while (candidate != null) {
            assert candidate.status() == TaskState.Status.WAITING;

            /*
            if (limitByUser.getOrDefault(task.userId(), 0) >= config.getUserLimit()) {
                LOG.warn("Can't execute another task {} for user {}", task.id(), task.userId());
                break;
            }

            if (limitByWorkflow.getOrDefault(task.executionId(), 0) >= config.getWorkflowLimit()) {
                LOG.warn("Can't execute another task {} for workflow {}", task.id(), task.executionId());
                break;
            }

            limitByUser.merge(task.userId(), 1, Integer::sum);
            limitByWorkflow.merge(task.executionId(), 1, Integer::sum);
             */

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

            readyTasks.poll();
            runningTask.put(task.id(), task);

            taskOnStatusChanged.accept(task);
            operationsExecutor.startNew(executeTaskAction);

            candidate = readyTasks.peek();
        }
    }

    private void completeTask(TaskState task) {
        //limitByUser.merge(task.userId(), -1, Integer::sum);
        //limitByWorkflow.merge(task.executionId(), -1, Integer::sum);

        runningTask.remove(task.id());

        for (var taskId : task.tasksDependedFrom()) {
            var waitingTask = waitingTasks.get(taskId);
            waitingTask.tasksDependedOn().remove(task.id());

            if (waitingTask.tasksDependedOn().isEmpty()) {
                waitingTasks.remove(taskId);
                readyTasks.add(waitingTask);
            }
        }
        taskOnStatusChanged.accept(task);
    }

    private void restoreTasks(String instanceId) {
        try {
            withRetries(LOG, () -> {
                List<TaskState> taskList = taskDao.getTasksByInstance(instanceId);

                List<String> failedGraphs = taskList.stream()
                    .filter(task -> task.status() == TaskState.Status.FAILED)
                    .map(TaskState::graphId)
                    .distinct()
                    .toList();

                if (!failedGraphs.isEmpty()) {
                    LOG.error("Found failed tasks for not failed graphs {}", failedGraphs);
                    throw new RuntimeException();
                }

                List<TaskState> completedTasks = taskList.stream()
                    .filter(task -> task.status() == TaskState.Status.COMPLETED)
                    .toList();
                Map<String, TaskState> tasksById = taskList.stream()
                    .filter(task -> task.status() == TaskState.Status.WAITING)
                    .collect(Collectors.toMap(TaskState::id, task -> task));

                for (var task: completedTasks) {
                    for (var depTaskId: task.tasksDependedFrom()) {
                        TaskState depTask = tasksById.get(depTaskId);
                        depTask.tasksDependedOn().remove(task.id());
                    }
                }

                tasksById.values().stream()
                    .filter(task -> task.tasksDependedOn().isEmpty())
                    .forEach(readyTasks::add);

                tasksById.values().stream()
                    .filter(task -> !task.tasksDependedOn().isEmpty())
                    .forEach(task -> waitingTasks.put(task.id(), task));

                var statuses = Set.of(TaskState.Status.WAITING_ALLOCATION,
                    TaskState.Status.ALLOCATING, TaskState.Status.EXECUTING);
                List<TaskState> executingTasks = taskList.stream()
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
