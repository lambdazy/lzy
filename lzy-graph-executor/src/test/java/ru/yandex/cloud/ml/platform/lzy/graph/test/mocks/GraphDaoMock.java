package ru.yandex.cloud.ml.platform.lzy.graph.test.mocks;
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
import yandex.cloud.priv.datasphere.v2.lzy.Tasks;


public class GraphDaoMock implements GraphExecutionDao {
    private final ConcurrentHashMap<Key, GraphExecutionState> storage = new ConcurrentHashMap<>();

    @Override
    public GraphExecutionState create(String workflowId, GraphDescription description) {
        var graph = new GraphExecutionState(workflowId, UUID.randomUUID().toString(), description);
        storage.put(new Key(workflowId, graph.id()), graph);
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

    @Override
    public synchronized void updateAtomic(String workflowId, String graphExecutionId, Mapper mapper) {
        try {
            GraphExecutionState graph = mapper.update(storage.get(new Key(workflowId, graphExecutionId)));
            storage.put(new Key(workflowId, graphExecutionId), graph);
            notifyAll();
        } catch (Exception ignored) {
        }

    }

    @Override
    public synchronized void updateAtomic(Set<GraphExecutionState.Status> statuses, Mapper mapper) {
        storage
            .values()
            .stream()
            .filter(t -> statuses.contains(t.status()))
            .limit(1)
            .map(t -> {
                try {
                    return mapper.update(t);
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            })
            .filter(Objects::nonNull)
            .forEach(t -> storage.put(new Key(t.workflowId(), t.id()), t));
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

    private record Key(String workflowId, String graphId) {}
}
