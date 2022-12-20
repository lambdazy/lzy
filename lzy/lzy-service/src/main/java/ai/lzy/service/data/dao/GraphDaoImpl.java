package ai.lzy.service.data.dao;

import ai.lzy.model.db.DbOperation;
import ai.lzy.model.db.Storage;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.service.data.storage.LzyServiceStorage;
import jakarta.inject.Singleton;

import java.sql.SQLException;
import java.util.Arrays;
import javax.annotation.Nullable;

@Singleton
public class GraphDaoImpl implements GraphDao {
    private final Storage storage;

    public GraphDaoImpl(LzyServiceStorage storage) {
        this.storage = storage;
    }

    @Override
    public void putJsonState(String graphOpId, String jsonState, @Nullable TransactionHandle transaction)
        throws SQLException
    {
        DbOperation.execute(transaction, storage, connection -> {
            try (var statement = connection.prepareStatement("""
                INSERT INTO graph_op_state (op_id, state_json)
                VALUES (?, ?)"""))
            {
                statement.setString(1, graphOpId);
                statement.setString(2, jsonState);
                statement.execute();
            }
        });
    }

    @Override
    public void updateJsonState(String graphOpId, String jsonState, @Nullable TransactionHandle transaction)
        throws SQLException
    {
        DbOperation.execute(transaction, storage, connection -> {
            try (var statement = connection.prepareStatement("""
                UPDATE graph_op_state
                SET state_json = ?
                WHERE op_id = ?"""))
            {
                statement.setString(1, jsonState);
                statement.setString(2, graphOpId);
                statement.execute();
            }
        });
    }

    @Nullable
    @Override
    public String getJsonState(String graphOpId, @Nullable TransactionHandle transaction) throws SQLException {
        String[] jsonState = {null};
        DbOperation.execute(transaction, storage, connection -> {
            try (var statement = connection.prepareStatement("""
                SELECT state_json FROM graph_op_state
                WHERE op_id = ?"""))
            {
                statement.setString(1, graphOpId);
                var rs = statement.executeQuery();
                if (rs.next()) {
                    jsonState[0] = rs.getString(1);
                }
            }
        });
        return jsonState[0];
    }

    @Override
    public void save(GraphDescription description, @Nullable TransactionHandle transaction) throws SQLException {

        DbOperation.execute(transaction, storage, con -> {
            try (var statement = con.prepareStatement("""
                INSERT INTO graphs (graph_id, execution_id, portal_input_slots)
                VALUES (?, ? ,?)
                """))
            {
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

    @Nullable
    @Override
    public GraphDescription get(String graphId, String executionId,
                                @Nullable TransactionHandle transaction) throws SQLException
    {

        GraphDescription[] desc = {null};
        DbOperation.execute(transaction, storage, con -> {
            try (var statement = con.prepareStatement("""
                SELECT (portal_input_slots) FROM graphs
                WHERE graph_id = ? AND execution_id = ?
                """))
            {
                statement.setString(1, graphId);
                statement.setString(2, executionId);
                var rs = statement.executeQuery();
                if (rs.next()) {
                    desc[0] = new GraphDescription(
                        graphId, executionId,
                        Arrays.stream(((String[]) rs.getArray(1).getArray())).toList()
                    );
                }
            }
        });
        return desc[0];
    }
}
