package ai.lzy.graph.db;

import ai.lzy.graph.model.Graph;
import ai.lzy.model.db.TransactionHandle;
import jakarta.annotation.Nullable;

import java.sql.SQLException;
import java.util.List;

public interface GraphDao {
    void createOrUpdate(Graph graph, @Nullable TransactionHandle transaction) throws SQLException;
    Graph getById(String graphId) throws SQLException;
    List<Graph> getByInstance(String instanceId) throws SQLException;
}
