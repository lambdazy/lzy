package ai.lzy.graph.db;

import ai.lzy.graph.model.GraphDescription;
import ai.lzy.graph.model.GraphExecutionState;
import ai.lzy.model.db.exceptions.DaoException;

import java.util.List;
import javax.annotation.Nullable;

public interface GraphExecutionDao {
    GraphExecutionState create(String workflowId, String workflowName, String userId,
                               GraphDescription description) throws DaoException;

    @Nullable
    GraphExecutionState get(String workflowId, String graphExecutionId) throws DaoException;
    List<GraphExecutionState> filter(GraphExecutionState.Status status) throws DaoException;
    List<GraphExecutionState> list(String workflowId) throws DaoException;

    @Nullable
    GraphExecutionState acquire(String workflowId, String graphExecutionId) throws DaoException;

    void updateAndFree(GraphExecutionState graph) throws DaoException;

}
