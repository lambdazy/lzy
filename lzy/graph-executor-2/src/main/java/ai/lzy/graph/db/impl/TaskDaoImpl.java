package ai.lzy.graph.db.impl;

import ai.lzy.graph.config.ServiceConfig;
import ai.lzy.graph.db.TaskDao;
import ai.lzy.graph.model.Task;
import ai.lzy.graph.model.TaskOperation;
import ai.lzy.model.db.TransactionHandle;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;
import java.util.List;

@Singleton
public class TaskDaoImpl implements TaskDao {
    private static final Logger LOG = LogManager.getLogger(TaskDaoImpl.class);

    private final GraphExecutorDataSource storage;
    private final ObjectMapper objectMapper;
    private final ServiceConfig config;

    @Inject
    public TaskDaoImpl(GraphExecutorDataSource storage, ServiceConfig config) {
        this.storage = storage;
        this.config = config;
        this.objectMapper = new ObjectMapper().findAndRegisterModules();
    }

    @Override
    public void createOrUpdateTask(Task task, TransactionHandle transaction) throws SQLException {

    }

    @Override
    public Task getTaskById(String taskId) throws SQLException {
        return null;
    }

    @Override
    public List<Task> getTasksByGraph(String graphId) throws SQLException {
        return null;
    }

    @Override
    public List<Task> getTasksByInstance(String instanceId) throws SQLException {
        return null;
    }

    @Override
    public void createOrUpdateTaskOperation(TaskOperation taskOperation, TransactionHandle transaction)
        throws SQLException
    {

    }

    @Override
    public TaskOperation getTaskOperationById(String taskOperationId) throws SQLException {
        return null;
    }

    @Override
    public List<TaskOperation> getTasksOperationsByInstance(String instanceId) throws SQLException {
        return null;
    }
}
