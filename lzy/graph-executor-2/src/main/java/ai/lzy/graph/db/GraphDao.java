package ai.lzy.graph.db;

import ai.lzy.graph.model.GraphState;
import ai.lzy.model.db.TransactionHandle;
import jakarta.annotation.Nullable;

import java.sql.SQLException;
import java.util.List;

public interface GraphDao {
    void create(GraphState graph, @Nullable TransactionHandle transaction) throws SQLException;

    void update(GraphState graph, @Nullable TransactionHandle transaction) throws SQLException;

    @Nullable
    GraphState getById(String graphId) throws SQLException;

    List<GraphState> loadActiveGraphs(String instanceId) throws SQLException;
}
