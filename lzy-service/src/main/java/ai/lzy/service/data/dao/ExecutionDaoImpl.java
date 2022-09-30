package ai.lzy.service.data.dao;

import ai.lzy.model.db.DbOperation;
import ai.lzy.model.db.Storage;
import ai.lzy.service.data.storage.LzyServiceStorage;
import ai.lzy.util.grpc.JsonUtils;
import jakarta.inject.Singleton;
import org.apache.commons.collections4.SetUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

@Singleton
public class ExecutionDaoImpl implements ExecutionDao {
    private static final Logger LOG = LogManager.getLogger(ExecutionDaoImpl.class);

    private static final String QUERY_EXISTING_SLOTS_IN_EXECUTION = """
        SELECT slot_uri
        FROM slot_snapshots
        WHERE execution_id = ? AND slot_uri IN %s""";

    private static final String QUERY_PUT_SLOTS_URI = """
        INSERT INTO slot_snapshots (slot_uri, execution_id) 
        VALUES (?, ?)""";

    private static final String QUERY_FIND_EXISTING_SLOTS = """
        SELECT slot_uri
        FROM slot_snapshots
        WHERE slot_uri IN %s""";

    private final Storage storage;

    public ExecutionDaoImpl(LzyServiceStorage storage) {
        this.storage = storage;
    }

    @Override
    public void saveSlots(String executionId, Set<String> slotsUri) throws SQLException {
        DbOperation.execute(null, storage, con -> {
            try (var statement = con.prepareStatement(QUERY_PUT_SLOTS_URI)) {
                for (var slotUri : slotsUri) {
                    statement.setString(1, slotUri);
                    statement.setString(2, executionId);
                    statement.addBatch();
                    statement.clearParameters();
                }
                statement.executeBatch();
            }
        });
    }

    @Override
    public Set<String> retainExistingSlots(Set<String> slotsUri) throws SQLException {
        var existingSlots = new HashSet<String>();

        DbOperation.execute(null, storage, con -> {
            String slotsAsString = JsonUtils.printAsTuple(slotsUri);

            try (var statement = con.prepareStatement(QUERY_FIND_EXISTING_SLOTS.formatted(slotsAsString))) {
                ResultSet rs = statement.executeQuery();
                while (rs.next()) {
                    existingSlots.add(rs.getString("slot_uri"));
                }
            }
        });

        return existingSlots;
    }

    @Override
    public Set<String> retainNonExistingSlots(String executionId, Set<String> slotsUri) throws SQLException {
        var existingSlots = new HashSet<String>();

        DbOperation.execute(null, storage, con -> {
            String slotsAsString = JsonUtils.printAsTuple(slotsUri);

            try (var statement = con.prepareStatement(QUERY_EXISTING_SLOTS_IN_EXECUTION.formatted(slotsAsString))) {
                statement.setString(1, executionId);
                ResultSet rs = statement.executeQuery();
                while (rs.next()) {
                    existingSlots.add(rs.getString("slot_uri"));
                }
            }
        });

        return SetUtils.difference(slotsUri, existingSlots);
    }
}
