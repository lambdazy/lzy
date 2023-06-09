package ai.lzy.service.dao.impl;

import ai.lzy.model.db.DbOperation;
import ai.lzy.model.db.Storage;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.service.dao.GraphDao;
import jakarta.annotation.Nullable;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Singleton
public class GraphDaoImpl implements GraphDao {
    private static final Logger LOG = LogManager.getLogger(GraphDaoImpl.class);

    private static final String QUERY_INSERT_GRAPH_DESCRIPTION = """
        INSERT INTO graphs (graph_id, execution_id, portal_input_slots)
        VALUES (?, ? ,?) ON CONFLICT DO NOTHING""";

    public static final String QUERY_SELECT_GRAPH_DESCRIPTION = """
        SELECT portal_input_slots FROM graphs
        WHERE graph_id = ? AND execution_id = ?""";

    public static final String QUERY_SELECT_GRAPHS_DESCRIPTIONS = """
        SELECT graph_id, portal_input_slots FROM graphs
        WHERE execution_id = ?""";

    private final Storage storage;

    public GraphDaoImpl(LzyServiceStorage storage) {
        this.storage = storage;
    }

    @Override
    public void put(GraphDescription description, @Nullable TransactionHandle transaction) throws SQLException {
        LOG.debug("Put graph description: { desc: {} }", description.toJson());

        DbOperation.execute(transaction, storage, con -> {
            try (var statement = con.prepareStatement(QUERY_INSERT_GRAPH_DESCRIPTION)) {
                statement.setString(1, description.graphId());
                statement.setString(2, description.executionId());
                statement.setArray(3, con.createArrayOf(
                    "text",
                    description.portalInputSlotNames().toArray(new String[0])
                ));
                statement.execute();
            }
        });
    }

    @Override
    @Nullable
    public GraphDescription get(String graphId, String execId) throws SQLException {
        return DbOperation.execute(null, storage, connection -> {
            try (var st = connection.prepareStatement(QUERY_SELECT_GRAPH_DESCRIPTION)) {
                st.setString(1, graphId);
                st.setString(2, execId);
                var rs = st.executeQuery();
                if (rs.next()) {
                    return new GraphDescription(graphId, execId,
                        Arrays.stream(((String[]) rs.getArray(1).getArray())).toList()
                    );
                }
                return null;
            }
        });
    }

    @Override
    public List<GraphDescription> getAll(String execId) throws SQLException {
        var graphs = new ArrayList<GraphDescription>();

        DbOperation.execute(null, storage, connection -> {
            try (var st = connection.prepareStatement(QUERY_SELECT_GRAPHS_DESCRIPTIONS)) {
                st.setString(1, execId);
                var rs = st.executeQuery();
                while (rs.next()) {
                    graphs.add(new GraphDescription(rs.getString(1), execId,
                        Arrays.stream(((String[]) rs.getArray(2).getArray())).toList())
                    );
                }
            }
        });

        return graphs;
    }
}
