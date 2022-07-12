package ai.lzy.graph.test.mocks;
import ai.lzy.graph.db.impl.GraphExecutionDaoImpl;
import jakarta.inject.Inject;
import java.util.Set;
import java.util.stream.Collectors;
import ai.lzy.graph.db.DaoException;
import ai.lzy.graph.db.Storage;
import ai.lzy.graph.model.GraphDescription;
import ai.lzy.graph.model.GraphExecutionState;

import ai.lzy.graph.model.TaskExecution;


public class GraphDaoMock extends GraphExecutionDaoImpl {
    @Inject
    public GraphDaoMock(Storage storage) {
        super(storage);
    }

    @Override
    public synchronized GraphExecutionState create(String workflowId, String workflowName,
                                                   GraphDescription description) throws DaoException {
        var graph = super.create(workflowId, workflowName, description);
        this.notifyAll();
        return graph;
    }

    public void waitForStatus(String workflowId, String graphId, GraphExecutionState.Status status)
            throws InterruptedException, DaoException {
        GraphExecutionState currentState = this.get(workflowId, graphId);
        while (currentState == null || currentState.status() != status) {
            Thread.sleep(10);
            currentState = this.get(workflowId, graphId);
        }
    }

    public synchronized void waitForExecutingNow(String workflowId, String graphId,
                                           Set<String> executions) throws InterruptedException, DaoException {
        GraphExecutionState currentState = this.get(workflowId, graphId);
        while (currentState == null
            || !currentState.currentExecutionGroup()
            .stream()
            .map(TaskExecution::id)
            .collect(Collectors.toSet()).equals(executions)) {
            Thread.sleep(10);
            currentState = this.get(workflowId, graphId);
        }
    }

    private record Key(String workflowId, String graphId) {}
}
