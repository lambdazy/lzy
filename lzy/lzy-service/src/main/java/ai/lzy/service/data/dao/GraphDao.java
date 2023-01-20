package ai.lzy.service.data.dao;

import ai.lzy.model.db.TransactionHandle;
import ai.lzy.service.graph.GraphExecutionState;
import jakarta.annotation.Nullable;

import java.sql.SQLException;
import java.util.List;

public interface GraphDao {
    void put(GraphExecutionState state, String ownerId, @Nullable TransactionHandle transaction)
        throws SQLException;

    void update(GraphExecutionState state, @Nullable TransactionHandle transaction)
        throws SQLException;

    List<GraphExecutionState> loadNotCompletedOpStates(String ownerId, @Nullable TransactionHandle transaction)
        throws SQLException;

    void save(GraphDescription description, @Nullable TransactionHandle transaction) throws SQLException;

    @Nullable
    GraphDescription get(String graphId, String executionId) throws SQLException;

    List<GraphDescription> getAll(String executionId) throws SQLException;

    record GraphDescription(
        String graphId,
        String executionId,
        List<String> portalInputSlotNames
    ) {}
}
