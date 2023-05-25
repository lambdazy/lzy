package ai.lzy.graph.db;

import java.sql.SQLException;
import java.util.List;

import ai.lzy.graph.model.Graph;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.DaoException;
import jakarta.annotation.Nullable;

public interface GraphDao {
    void createOrUpdate(Graph graph, @Nullable TransactionHandle transaction) throws SQLException;
    Graph getById(String graphId) throws DaoException;
    List<Graph> getByInstance(String instanceId) throws DaoException;
}
