package ru.yandex.cloud.ml.platform.lzy.graph_executor.test.mocks;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import ru.yandex.cloud.ml.platform.lzy.graph_executor.db.GraphExecutionDao;
import ru.yandex.cloud.ml.platform.lzy.graph_executor.model.GraphDescription;
import ru.yandex.cloud.ml.platform.lzy.graph_executor.model.GraphExecutionState;

import javax.annotation.Nullable;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;


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
        } catch (Exception ignored) {
        }

    }

    @Override
    public synchronized void updateListAtomic(GraphExecutionState.Status status, ParallelMapper mapper, int limit) throws GraphDaoException {
        filter(status).stream().limit(limit).map(mapper::update).map(t -> {
            try {
                return t.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
                return null;
            }
        }).filter(Objects::nonNull).forEach(t -> storage.put(new Key(t.workflowId(), t.id()), t));
    }

    private record Key(String workflowId, String graphId) {}
}
