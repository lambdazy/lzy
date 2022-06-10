package ru.yandex.cloud.ml.platform.lzy.graph.test.mocks;
import java.time.temporal.TemporalUnit;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import ru.yandex.cloud.ml.platform.lzy.graph.db.GraphExecutionDao;
import ru.yandex.cloud.ml.platform.lzy.graph.model.GraphDescription;
import ru.yandex.cloud.ml.platform.lzy.graph.model.GraphExecutionState;

import javax.annotation.Nullable;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import ru.yandex.cloud.ml.platform.lzy.graph.model.TaskExecution;
import yandex.cloud.priv.datasphere.v2.lzy.Tasks;


public class GraphDaoMock implements GraphExecutionDao {
    private final ConcurrentHashMap<Key, GraphExecutionState> storage = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Key, Boolean> acquired = new ConcurrentHashMap<>();

    @Override
    public GraphExecutionState create(String workflowId, GraphDescription description) {
        var graph = GraphExecutionState.builder()
            .withWorkflowId(workflowId)
            .withId(UUID.randomUUID().toString())
            .withDescription(description)
            .build();
        storage.put(new Key(workflowId, graph.id()), graph);
        acquired.put(new Key(workflowId, graph.id()), false);
        return graph;
    }

    @Nullable
    @Override
    public synchronized GraphExecutionState get(String workflowId, String graphExecutionId) {
        return storage.get(new Key(workflowId, graphExecutionId));
    }

    @Override
    public List<GraphExecutionState> filter(GraphExecutionState.Status status) {
        return storage.values().stream().filter(t -> t.status() == status).collect(Collectors.toList());
    }

    @Override
    public List<GraphExecutionState> list(String workflowId) {
        return storage.values().stream().toList();
    }

    @org.jetbrains.annotations.Nullable
    @Override
    public GraphExecutionState acquire(String workflowId, String graphExecutionId, long upTo, TemporalUnit unit)
        throws GraphDaoException {
        if (acquired.get(new Key(workflowId, graphExecutionId))) {
            throw new GraphDaoException(
                String.format("Cannot acquire graph <%s> in workflow <%s>", graphExecutionId, workflowId)
            );
        }
        acquired.put(new Key(workflowId, graphExecutionId), true);
        return storage.get(new Key(workflowId, graphExecutionId));
    }

    @Override
    public void free(GraphExecutionState graph) throws GraphDaoException {
        acquired.put(new Key(graph.workflowId(), graph.id()), false);
        storage.put(new Key(graph.workflowId(), graph.id()), graph);
    }

    public synchronized void waitForStatus(String workflowId, String graphId,
                                           GraphExecutionState.Status status,
                                           int timeoutMillis) throws InterruptedException {
        long startTime = System.currentTimeMillis();
        while (
            (
                storage.get(new Key(workflowId, graphId)) == null
                || storage.get(new Key(workflowId, graphId)).status() != status
            )
                && System.currentTimeMillis() - startTime < timeoutMillis
        ) {
            this.wait(timeoutMillis);
        }
        if (
            storage.get(new Key(workflowId, graphId)) == null
            || storage.get(new Key(workflowId, graphId)).status() != status
        ) {
            throw new RuntimeException("Timeout exceeded");
        }
    }

    public synchronized void waitForExecutingNow(String workflowId, String graphId,
                                           Set<String> executions,
                                           int timeoutMillis) throws InterruptedException {
        long startTime = System.currentTimeMillis();
        while (
            (
                storage.get(new Key(workflowId, graphId)) == null
                    || !storage.get(new Key(workflowId, graphId))
                    .currentExecutionGroup()
                    .stream()
                    .map(TaskExecution::id)
                    .collect(Collectors.toSet()).equals(executions)
            )
                && System.currentTimeMillis() - startTime < timeoutMillis
        ) {
            this.wait(timeoutMillis);
        }
        if (
            storage.get(new Key(workflowId, graphId)) == null
                || !storage.get(new Key(workflowId, graphId))
                .currentExecutionGroup()
                .stream()
                .map(TaskExecution::id)
                .collect(Collectors.toSet()).equals(executions)
        ) {
            throw new RuntimeException("Timeout exceeded");
        }
    }

    private record Key(String workflowId, String graphId) {}
}
