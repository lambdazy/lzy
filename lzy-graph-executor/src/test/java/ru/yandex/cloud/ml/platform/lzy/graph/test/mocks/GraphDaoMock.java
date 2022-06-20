package ru.yandex.cloud.ml.platform.lzy.graph.test.mocks;
import jakarta.inject.Inject;
import java.util.Set;
import java.util.stream.Collectors;
import ru.yandex.cloud.ml.platform.lzy.graph.db.DaoException;
import ru.yandex.cloud.ml.platform.lzy.graph.db.GraphExecutionDao;
import ru.yandex.cloud.ml.platform.lzy.graph.db.Storage;
import ru.yandex.cloud.ml.platform.lzy.graph.db.impl.GraphExecutionDaoImpl;
import ru.yandex.cloud.ml.platform.lzy.graph.model.GraphDescription;
import ru.yandex.cloud.ml.platform.lzy.graph.model.GraphExecutionState;

import javax.annotation.Nullable;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import ru.yandex.cloud.ml.platform.lzy.graph.model.TaskExecution;


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
