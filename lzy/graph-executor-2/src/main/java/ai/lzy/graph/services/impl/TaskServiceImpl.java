package ai.lzy.graph.services.impl;

import ai.lzy.graph.GraphExecutorApi2;
import ai.lzy.graph.config.ServiceConfig;
import ai.lzy.graph.db.TaskDao;
import ai.lzy.graph.model.Task;
import ai.lzy.graph.services.TaskService;
import ai.lzy.longrunning.OperationsExecutor;
import jakarta.annotation.Nullable;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

@Singleton
public class TaskServiceImpl implements TaskService {
    private static final Logger LOG = LogManager.getLogger(TaskServiceImpl.class);

    private final TaskDao taskDao;
    private final OperationsExecutor operationsExecutor;
    private final ServiceConfig config;

    private final PriorityQueue<Task> readyTasks = new PriorityQueue<>();
    private final Set<Task> waitingTasks = new HashSet<>();
    private final Map<String, Integer> limitByUser = new ConcurrentHashMap<>();
    private final Map<String, Integer> limitByWorkflow = new ConcurrentHashMap<>();
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

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
    public void addTask(Task task, Consumer<Task> onComplete) {

    }

    @Override
    @Nullable
    public GraphExecutorApi2.TaskExecutionStatus getTaskStatus(String taskId) {
        return null;
    }

    private void restoreTasks(String instanceId) {
        try {
            taskDao.getTasksByInstance(instanceId);
            taskDao.getTasksOperationsByInstance(instanceId);
        } catch (SQLException e) {
            LOG.error(e);
        }
    }

    private void run() {
        while (true) {
            Task task = readyTasks.peek();
            if (task != null && limitByUser.get(task.userId()) < config.getUserLimit() &&
                limitByWorkflow.get(task.workflowId()) < config.getWorkflowLimit())
            {
                //create and execute task operation
            }
        }
    }
}
