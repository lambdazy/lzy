package ai.lzy.graph.db.impl;

import ai.lzy.graph.db.TaskDao;
import ai.lzy.graph.model.Task;
import ai.lzy.graph.model.TaskOperation;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.DaoException;
import jakarta.inject.Singleton;

import java.sql.SQLException;
import java.util.List;

@Singleton
public class TaskDaoImpl implements TaskDao {

    @Override
    public void createOrUpdateTask(Task task, TransactionHandle transaction) throws SQLException {

    }

    @Override
    public Task getTaskById(String taskId) throws DaoException {
        return null;
    }

    @Override
    public List<Task> getTasksByGraph(String graphId) throws DaoException {
        return null;
    }

    @Override
    public List<Task> getTasksByInstance(String instanceId) throws DaoException {
        return null;
    }

    @Override
    public void createOrUpdateTaskOperation(TaskOperation taskOperation, TransactionHandle transaction)
        throws SQLException
    {

    }

    @Override
    public TaskOperation getTaskOperationById(String taskOperationId) throws DaoException {
        return null;
    }

    @Override
    public List<TaskOperation> getTasksOperationsByInstance(String instanceId) throws DaoException {
        return null;
    }
}
