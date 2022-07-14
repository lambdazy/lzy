package ai.lzy.graph.db;

import ai.lzy.graph.model.GraphDescription;
import ai.lzy.graph.model.GraphExecutionState;
import ai.lzy.model.db.DaoException;

import javax.annotation.Nullable;
import java.util.List;

public interface GraphExecutionDao {
    GraphExecutionState create(String workflowId, String workflowName,
                               GraphDescription description) throws DaoException;

    @Nullable
    GraphExecutionState get(String workflowId, String graphExecutionId) throws DaoException;
    List<GraphExecutionState> filter(GraphExecutionState.Status status) throws DaoException;
    List<GraphExecutionState> list(String workflowId) throws DaoException;

    @Nullable
    GraphExecutionState acquire(String workflowId, String graphExecutionId) throws DaoException;

    void free(GraphExecutionState graph) throws DaoException;

}
