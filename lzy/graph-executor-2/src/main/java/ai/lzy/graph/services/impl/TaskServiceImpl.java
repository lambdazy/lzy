package ai.lzy.graph.services.impl;

import ai.lzy.graph.GraphExecutorApi2;
import ai.lzy.graph.config.ServiceConfig;
import ai.lzy.graph.db.TaskDao;
import ai.lzy.graph.db.impl.GraphExecutorDataSource;
import ai.lzy.graph.model.TaskState;
import ai.lzy.graph.services.AllocatorService;
import ai.lzy.graph.services.TaskService;
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

import java.util.*;
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

    private final TaskDao taskDao;
    private final GraphExecutorDataSource storage;
    private final OperationDao operationDao;
    private final OperationsExecutor operationsExecutor;
    private final AllocatorService allocatorService;
    private final ServiceConfig config;

    private final PriorityQueue<TaskState> readyTasks = new PriorityQueue<>(
        Comparator.comparingInt(task -> task.tasksDependedFrom().size()));
    private final Map<String, TaskState> waitingTasks = new ConcurrentHashMap<>();
    private final Map<String, Integer> limitByUser = new ConcurrentHashMap<>();
    private final Map<String, Integer> limitByWorkflow = new ConcurrentHashMap<>();
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    private volatile Consumer<TaskState> taskOnStatusChanged;

    @Inject
    public TaskServiceImpl(ServiceConfig config, TaskDao taskDao,
                           GraphExecutorDataSource storage,
                           AllocatorService allocatorService,
                           @Named("GraphExecutorOperationDao") OperationDao operationDao,
                           @Named("GraphExecutorOperationsExecutor") OperationsExecutor operationsExecutor)
    {
        this.taskDao = taskDao;
        this.operationsExecutor = operationsExecutor;
        this.storage = storage;
        this.operationDao = operationDao;
        this.allocatorService = allocatorService;
        this.config = config;

        restoreTasks(config.getInstanceId());
        executor.scheduleAtFixedRate(this::run, 1, 1, TimeUnit.SECONDS);
    }

    @Override
    public void init(Consumer<TaskState> taskOnStatusChanged) {
        this.taskOnStatusChanged = taskOnStatusChanged;
    }

    @Override
    public void addTasks(List<TaskState> tasks) {
        tasks.stream()
            .filter(task -> task.tasksDependedOn().isEmpty())
            .forEach(readyTasks::add);
        tasks.stream()
            .filter(task -> !task.tasksDependedOn().isEmpty())
            .forEach(task -> waitingTasks.put(task.id(), task));
    }

    @Override
    public void completeTask(TaskState task) {
        limitByUser.merge(task.userId(), -1, Integer::sum);
        limitByWorkflow.merge(task.executionId(), -1, Integer::sum);

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

    @Override
    @Nullable
    public GraphExecutorApi2.TaskExecutionStatus getTaskStatus(String taskId) {
        try {
            return taskDao.getTaskById(taskId).toProtoStatus();
        } catch (Exception e) {
            LOG.error("Couldn't find task {}", taskId);
            return null;
        }
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
                        storage, operationDao, operationsExecutor, taskDao, allocatorService, this::completeTask);
                    operationsExecutor.startNew(executeTaskAction);
                }
            });
        } catch (Exception e) {
            LOG.error("Cannot restore tasks for instance {}", instanceId);
            throw new RuntimeException(e);
        }
    }

    private void run() {
        TaskState task = readyTasks.peek();
        while (task != null) {
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

            var op = Operation.create(
                task.userId(),
                "Execute task '%s' of graph '%s'".formatted(task.id(), task.graphId()),
                null, null, null);

            TaskState finalTask = task.toBuilder()
                .status(TaskState.Status.WAITING_ALLOCATION)
                .executingState(task.executingState().toBuilder()
                    .opId(op.id())
                    .build())
                .build();

            ExecuteTaskAction executeTaskAction = new ExecuteTaskAction(op.id(), finalTask, "",
                storage, operationDao, operationsExecutor, taskDao, allocatorService, this::completeTask);

            try {
                withRetries(LOG, () -> {
                    try (var tx = TransactionHandle.create(storage)) {
                        operationDao.create(op, tx);
                        taskDao.updateTask(finalTask, tx);
                        tx.commit();
                    }
                });
            } catch (Exception e) {
                LOG.error("Couldn't update task {}, trying again.", task.id());
                break;
            }

            operationsExecutor.startNew(executeTaskAction);
            taskOnStatusChanged.accept(finalTask);
            LOG.info("Created task operation {} for task {}", op.id(), task.id());
            readyTasks.poll();
            task = readyTasks.peek();
        }
    }
}
