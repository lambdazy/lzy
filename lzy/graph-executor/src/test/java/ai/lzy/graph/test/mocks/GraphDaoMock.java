package ai.lzy.graph.test.mocks;

import ai.lzy.graph.db.impl.GraphExecutionDaoImpl;
import ai.lzy.graph.db.impl.GraphExecutorDataSource;
import ai.lzy.graph.model.GraphDescription;
import ai.lzy.graph.model.GraphExecutionState;
import ai.lzy.graph.model.TaskExecution;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.DaoException;
import jakarta.inject.Inject;

import java.sql.SQLException;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public class GraphDaoMock extends GraphExecutionDaoImpl {
    @Inject
    public GraphDaoMock(GraphExecutorDataSource storage) {
        super(storage);
    }

    @Override
    public synchronized GraphExecutionState create(String workflowId, String workflowName,
                                                   String userId, GraphDescription description,
                                                   @Nullable TransactionHandle transaction) throws SQLException
    {
        var graph = super.create(workflowId, workflowName, userId, description, transaction);
        this.notifyAll();
        return graph;
    }

    public void waitForStatus(String workflowId, String graphId, GraphExecutionState.Status status)
        throws InterruptedException, DaoException
    {
        GraphExecutionState currentState = this.get(workflowId, graphId);
        while (currentState == null || currentState.status() != status) {
            Thread.sleep(100);
            currentState = this.get(workflowId, graphId);
        }
    }

    public synchronized void waitForExecutingNow(String workflowId, String graphId,
                                                 Set<String> executions) throws InterruptedException, DaoException
    {
        GraphExecutionState currentState = this.get(workflowId, graphId);
        while (currentState == null
            || !currentState.currentExecutionGroup()
            .stream()
            .map(TaskExecution::id)
            .collect(Collectors.toSet()).equals(executions)) {
            Thread.sleep(100);
            currentState = this.get(workflowId, graphId);
        }
    }

    private record Key(String workflowId, String graphId) {}
}
