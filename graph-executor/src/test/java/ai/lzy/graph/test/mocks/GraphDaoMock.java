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
    public synchronized GraphExecutionState create(String workflowId, GraphDescription description) throws DaoException {
        var graph = super.create(workflowId, description);
        this.notifyAll();
        return graph;
    }

    @org.jetbrains.annotations.Nullable
    @Override
    public synchronized GraphExecutionState acquire(String workflowId, String graphExecutionId)
        throws DaoException {
        var graph = super.acquire(workflowId, graphExecutionId);
        this.notifyAll();
        return graph;
    }

    @Override
    public synchronized void free(GraphExecutionState graph) throws DaoException {
        this.notifyAll();
        super.free(graph);
    }

    public synchronized void waitForStatus(String workflowId, String graphId,
                                           GraphExecutionState.Status status,
                                           int timeoutMillis) throws InterruptedException, DaoException {
        long startTime = System.currentTimeMillis();
        GraphExecutionState currentState = this.get(workflowId, graphId);
        while ((currentState == null || currentState.status() != status)
                && System.currentTimeMillis() - startTime < timeoutMillis) {
            this.wait(timeoutMillis);
            currentState = this.get(workflowId, graphId);
        }
        if (currentState == null || currentState.status() != status) {
            throw new RuntimeException("Timeout exceeded");
        }
    }

    public synchronized void waitForExecutingNow(String workflowId, String graphId,
                                           Set<String> executions,
                                           int timeoutMillis) throws InterruptedException, DaoException {
        long startTime = System.currentTimeMillis();
        GraphExecutionState currentState = this.get(workflowId, graphId);
        while ((currentState == null
            || !currentState.currentExecutionGroup()
            .stream()
            .map(TaskExecution::id)
            .collect(Collectors.toSet()).equals(executions))
            && System.currentTimeMillis() - startTime < timeoutMillis) {
            this.wait(timeoutMillis);
            currentState = this.get(workflowId, graphId);
        }
        if (currentState == null
            || !currentState.currentExecutionGroup()
            .stream()
            .map(TaskExecution::id)
            .collect(Collectors.toSet()).equals(executions)) {
            throw new RuntimeException("Timeout exceeded");
        }
    }

    private record Key(String workflowId, String graphId) {}
}
