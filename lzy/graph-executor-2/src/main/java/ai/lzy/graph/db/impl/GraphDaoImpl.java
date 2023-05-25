package ai.lzy.graph.db.impl;

import ai.lzy.graph.db.GraphDao;
import ai.lzy.graph.model.Graph;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.DaoException;
import jakarta.inject.Singleton;

import java.sql.SQLException;
import java.util.List;

@Singleton
public class GraphDaoImpl implements GraphDao {
    @Override
    public void createOrUpdate(Graph graph, TransactionHandle transaction) throws SQLException {

    }

    @Override
    public Graph getById(String graphId) throws DaoException {
        return null;
    }

    @Override
    public List<Graph> getByInstance(String instanceId) throws DaoException {
        return null;
    }
}
