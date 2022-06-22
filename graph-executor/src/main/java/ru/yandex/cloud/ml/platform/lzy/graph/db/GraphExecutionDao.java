package ru.yandex.cloud.ml.platform.lzy.graph.db;

import ru.yandex.cloud.ml.platform.lzy.graph.model.GraphDescription;
import ru.yandex.cloud.ml.platform.lzy.graph.model.GraphExecutionState;

import javax.annotation.Nullable;
import java.util.List;

public interface GraphExecutionDao {
    GraphExecutionState create(String workflowId, GraphDescription description) throws DaoException;

    @Nullable
    GraphExecutionState get(String workflowId, String graphExecutionId) throws DaoException;
    List<GraphExecutionState> filter(GraphExecutionState.Status status) throws DaoException;
    List<GraphExecutionState> list(String workflowId) throws DaoException;

    @Nullable
    GraphExecutionState acquire(String workflowId, String graphExecutionId) throws DaoException;

    void free(GraphExecutionState graph) throws DaoException;

}
