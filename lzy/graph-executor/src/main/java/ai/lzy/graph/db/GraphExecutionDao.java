package ai.lzy.graph.db;

import ai.lzy.graph.model.GraphDescription;
import ai.lzy.graph.model.GraphExecutionState;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.DaoException;
import jakarta.annotation.Nullable;

import java.sql.SQLException;
import java.util.List;

public interface GraphExecutionDao {
    GraphExecutionState create(String workflowId, String workflowName, String userId, String allocatorSessionId,
                               GraphDescription description, @Nullable TransactionHandle tx) throws SQLException;

    @Nullable
    GraphExecutionState get(String workflowId, String graphExecutionId) throws DaoException;
    List<GraphExecutionState> filter(GraphExecutionState.Status status) throws DaoException;
    List<GraphExecutionState> list(String workflowId) throws DaoException;

    @Nullable
    GraphExecutionState acquire(String workflowId, String graphExecutionId) throws DaoException;

    void updateAndFree(GraphExecutionState graph) throws DaoException;

}
