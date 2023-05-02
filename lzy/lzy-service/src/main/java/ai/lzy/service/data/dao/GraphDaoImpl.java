package ai.lzy.service.data.dao;

import ai.lzy.model.db.DbOperation;
import ai.lzy.model.db.Storage;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.service.data.storage.LzyServiceStorage;
import ai.lzy.service.graph.GraphExecutionState;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.Nullable;
import jakarta.inject.Named;
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

    private final Storage storage;
    private final ObjectMapper objectMapper;

    public GraphDaoImpl(LzyServiceStorage storage, @Named("GraphDaoObjectMapper") ObjectMapper objectMapper) {
        this.storage = storage;
        this.objectMapper = objectMapper;
    }

    @Override
    public void put(GraphExecutionState state, String ownerId, @Nullable TransactionHandle transaction)
        throws SQLException
    {
        LOG.info("Put graph execution state to dao: " + state.toString());

        DbOperation.execute(transaction, storage, connection -> {
            try (var statement = connection.prepareStatement("""
                INSERT INTO graph_op_state (op_id, state_json, owner_id)
                VALUES (?, ?, ?)"""))
            {
                statement.setString(1, state.getOpId());
                statement.setString(2, objectMapper.writeValueAsString(state));
                statement.setString(3, ownerId);
                statement.execute();
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Cannot dump graph execution state operation");
            }
        });
    }

    @Override
    public void update(GraphExecutionState state, @Nullable TransactionHandle transaction)
        throws SQLException
    {
        LOG.info("Update graph execution state to dao: " + state.toString());

        DbOperation.execute(transaction, storage, connection -> {
            try (var statement = connection.prepareStatement("""
                UPDATE graph_op_state
                SET state_json = ?
                WHERE op_id = ?"""))
            {
                statement.setString(1, objectMapper.writeValueAsString(state));
                statement.setString(2, state.getOpId());
                statement.execute();
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Cannot dump graph execution state operation");
            }
        });
    }

    @Override
    public List<GraphExecutionState> loadNotCompletedOpStates(String ownerId, @Nullable TransactionHandle transaction)
        throws SQLException
    {
        LOG.info("Load not completed execute graph operations by: { ownerId: {} }", ownerId);

        List<GraphExecutionState> states = new ArrayList<>();

        DbOperation.execute(transaction, storage, connection -> {
            try (var statement = connection.prepareStatement("""
                SELECT state_json FROM graph_op_state
                JOIN operation o ON graph_op_state.op_id = o.id
                WHERE o.done = FALSE AND owner_id = ?"""))
            {
                statement.setString(1, ownerId);
                var rs = statement.executeQuery();
                while (rs.next()) {
                    var stateJson = rs.getString("state_json");
                    var state = objectMapper.readValue(stateJson, GraphExecutionState.class);
                    states.add(state);
                }
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Cannot parse graph execution state: " + e.getMessage(), e);
            }
        });

        return states;
    }

    @Override
    public void save(GraphDescription description, @Nullable TransactionHandle transaction) throws SQLException {

        DbOperation.execute(transaction, storage, con -> {
            try (var statement = con.prepareStatement("""
                INSERT INTO graphs (graph_id, execution_id, portal_input_slots)
                VALUES (?, ? ,?) ON CONFLICT DO NOTHING
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

    @Override
    @Nullable
    public GraphDescription get(String graphId, String executionId) throws SQLException {
        GraphDescription[] desc = {null};

        DbOperation.execute(null, storage, connection -> {
            try (var statement = connection.prepareStatement("""
                SELECT portal_input_slots FROM graphs
                WHERE graph_id = ? AND execution_id = ?"""))
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

    @Override
    public List<GraphDescription> getAll(String executionId) throws SQLException {
        var graphs = new ArrayList<GraphDescription>();

        DbOperation.execute(null, storage, connection -> {
            try (var statement = connection.prepareStatement("""
                SELECT graph_id, portal_input_slots FROM graphs
                WHERE execution_id = ?"""))
            {
                statement.setString(1, executionId);
                var rs = statement.executeQuery();
                while (rs.next()) {
                    graphs.add(
                        new GraphDescription(rs.getString(1), executionId,
                            Arrays.stream(((String[]) rs.getArray(2).getArray())).toList())
                    );
                }
            }
        });

        return graphs;
    }
}
