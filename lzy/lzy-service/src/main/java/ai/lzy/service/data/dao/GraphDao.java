package ai.lzy.service.data.dao;

import ai.lzy.model.db.TransactionHandle;
import ai.lzy.service.graph.GraphExecutionState;

import java.sql.SQLException;
import java.util.List;
import javax.annotation.Nullable;

public interface GraphDao {
    void put(GraphExecutionState state, String ownerId, @Nullable TransactionHandle transaction)
        throws SQLException;

    void update(GraphExecutionState state, @Nullable TransactionHandle transaction)
        throws SQLException;

    List<GraphExecutionState> loadNotCompletedOpStates(String ownerId, @Nullable TransactionHandle transaction)
        throws SQLException;

    void save(GraphDescription description, @Nullable TransactionHandle transaction) throws SQLException;

    default void save(GraphDescription description) throws SQLException {
        save(description, null);
    }

    @Nullable
    GraphDescription get(String graphId, String executionId,
                         @Nullable TransactionHandle transaction) throws SQLException;

    @Nullable
    default GraphDescription get(String graphId, String executionId) throws SQLException {
        return get(graphId, executionId, null);
    }

    record GraphDescription(
        String graphId,
        String executionId,
        List<String> portalInputSlotNames
    ) {}
}
