package ai.lzy.graph.services.impl;

import ai.lzy.graph.GraphExecutorApi2;
import ai.lzy.graph.config.ServiceConfig;
import ai.lzy.graph.db.TaskDao;
import ai.lzy.graph.model.TaskOperation;
import ai.lzy.graph.model.TaskState;
import ai.lzy.graph.services.TaskService;
import ai.lzy.longrunning.OperationsExecutor;
import jakarta.annotation.Nullable;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static ai.lzy.model.db.DbHelper.withRetries;

@Singleton
public class TaskServiceImpl implements TaskService {
    private static final Logger LOG = LogManager.getLogger(TaskServiceImpl.class);

    private final TaskDao taskDao;
    private final OperationsExecutor operationsExecutor;
    private final ServiceConfig config;

    private final PriorityQueue<TaskState> readyTasks = new PriorityQueue<>(
        Comparator.comparingInt(task -> task.tasksDependedFrom().size()));
    private final Map<String, TaskState> waitingTasks = new ConcurrentHashMap<>();
    private final Map<String, Integer> limitByUser = new ConcurrentHashMap<>();
    private final Map<String, Integer> limitByWorkflow = new ConcurrentHashMap<>();
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    private Consumer<TaskState> taskOnComplete;

    @Inject
    public TaskServiceImpl(ServiceConfig config, TaskDao taskDao,
                           @Named("GraphExecutorOperationsExecutor") OperationsExecutor operationsExecutor)
    {
        this.taskDao = taskDao;
        this.operationsExecutor = operationsExecutor;
        this.config = config;

        restoreTasks(config.getInstanceId());
        executor.execute(this::run);
    }

    @Override
    public void init(Consumer<TaskState> onComplete) {
        this.taskOnComplete = onComplete;
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
    @Nullable
    public GraphExecutorApi2.TaskExecutionStatus getTaskStatus(String taskId) {
        return null;
    }

    private void restoreTasks(String instanceId) {
        try {
            withRetries(LOG, () -> {
                List<TaskState> taskList = taskDao.getTasksByInstance(instanceId);
                List<TaskOperation> operationList = taskDao.getTasksOperationsByInstance(instanceId);

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

                if (operationList.isEmpty()) {
                    LOG.info("No active task operations found for instance '{}'", instanceId);
                    return;
                }

                LOG.warn("Found {} not completed task operations on instance '{}'", operationList.size(), instanceId);
                for (var op : operationList) {
                    LOG.info("Restore {}", op);
                    operationsExecutor.startNew(op.deferredAction());
                }
            });
        } catch (Exception e) {
            LOG.error("Cannot restore tasks for instance {}", instanceId);
            throw new RuntimeException(e);
        }
    }

    private void run() {
        while (true) {
            TaskState task = readyTasks.peek();
            if (task != null && limitByUser.get(task.userId()) < config.getUserLimit() &&
                limitByWorkflow.get(task.workflowId()) < config.getWorkflowLimit())
            {
                //create and execute task operation
            }
        }
    }
}
