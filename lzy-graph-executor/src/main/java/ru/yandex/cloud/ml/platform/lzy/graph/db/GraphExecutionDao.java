package ru.yandex.cloud.ml.platform.lzy.graph.db;

import java.time.temporal.TemporalUnit;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import ru.yandex.cloud.ml.platform.lzy.graph.model.GraphDescription;
import ru.yandex.cloud.ml.platform.lzy.graph.model.GraphExecutionState;

import javax.annotation.Nullable;
import java.util.List;

public interface GraphExecutionDao {
    GraphExecutionState create(String workflowId, GraphDescription description) throws GraphDaoException;

    @Nullable
    GraphExecutionState get(String workflowId, String graphExecutionId) throws GraphDaoException;
    List<GraphExecutionState> filter(GraphExecutionState.Status status) throws GraphDaoException;
    List<GraphExecutionState> list(String workflowId) throws GraphDaoException;

    @Nullable
    GraphExecutionState acquire(String workflowId, String graphExecutionId,
                                long upTo, TemporalUnit unit) throws GraphDaoException;

    void free(GraphExecutionState graph) throws GraphDaoException;

    class GraphDaoException extends Exception {
        public GraphDaoException(Exception e) {
            super(e);
        }

        public GraphDaoException(String e) {
            super(e);
        }
    }
}
