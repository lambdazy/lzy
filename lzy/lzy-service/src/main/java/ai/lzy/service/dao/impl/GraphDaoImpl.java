package ai.lzy.service.dao.impl;

import ai.lzy.model.db.DbOperation;
import ai.lzy.model.db.Storage;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.service.dao.GraphDao;
import jakarta.annotation.Nullable;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@Singleton
public class GraphDaoImpl implements GraphDao {
    private static final Logger LOG = LogManager.getLogger(GraphDaoImpl.class);

    private static final String QUERY_INSERT_GRAPH_DESCRIPTION = """
        INSERT INTO graphs (graph_id, execution_id)
        VALUES (?, ?) ON CONFLICT DO NOTHING""";

    public static final String QUERY_SELECT_GRAPHS_DESCRIPTIONS = """
        SELECT graph_id FROM graphs
        WHERE execution_id = ?""";

    private final Storage storage;

    public GraphDaoImpl(LzyServiceStorage storage) {
        this.storage = storage;
    }

    @Override
    public void put(GraphDescription description, @Nullable TransactionHandle transaction) throws SQLException {
        LOG.debug("Put graph description: { desc: {} }", description.toJson());

        DbOperation.execute(transaction, storage, con -> {
            try (PreparedStatement statement = con.prepareStatement(QUERY_INSERT_GRAPH_DESCRIPTION)) {
                statement.setString(1, description.graphId());
                statement.setString(2, description.executionId());
                statement.execute();
            }
        });
    }

    @Override
    public List<GraphDescription> getAll(String execId) throws SQLException {
        var graphs = new ArrayList<GraphDescription>();

        DbOperation.execute(null, storage, connection -> {
            try (PreparedStatement st = connection.prepareStatement(QUERY_SELECT_GRAPHS_DESCRIPTIONS)) {
                st.setString(1, execId);
                var rs = st.executeQuery();
                while (rs.next()) {
                    graphs.add(new GraphDescription(rs.getString(1), execId)
                    );
                }
            }
        });

        return graphs;
    }
}
