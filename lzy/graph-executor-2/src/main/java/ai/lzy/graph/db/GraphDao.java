package ai.lzy.graph.db;

import ai.lzy.graph.model.Graph;
import ai.lzy.model.db.TransactionHandle;
import jakarta.annotation.Nullable;

import java.sql.SQLException;
import java.util.List;

public interface GraphDao {
    void create(Graph graph, @Nullable TransactionHandle transaction) throws SQLException;
    void update(Graph graph, @Nullable TransactionHandle transaction) throws SQLException;
    @Nullable
    Graph getById(String graphId) throws SQLException;
    @Nullable
    List<Graph> getByInstance(String instanceId) throws SQLException;
}
