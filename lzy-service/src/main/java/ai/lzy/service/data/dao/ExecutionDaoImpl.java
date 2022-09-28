package ai.lzy.service.data.dao;

import ai.lzy.model.db.DbOperation;
import ai.lzy.model.db.Storage;
import ai.lzy.service.data.storage.LzyServiceStorage;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

@Singleton
public class ExecutionDaoImpl implements ExecutionDao {
    private static final Logger LOG = LogManager.getLogger(ExecutionDaoImpl.class);

    private static final String QUERY_GET_SLOTS_URI = """
        SELECT slot_uri
        FROM slot_snapshots
        WHERE execution_id = ?""";

    private static final String QUERY_PUT_SLOTS_URI = """
        INSERT INTO slot_snapshots (slot_uri, execution_id) 
        VALUES (?, ?)""";

    private static final String QUERY_FIND_PRESENTED_SLOTS_URI = """
        SELECT slot_uri
        FROM slot_snapshots
        WHERE slot_uri IN %s""";

    private final Storage storage;

    public ExecutionDaoImpl(LzyServiceStorage storage) {
        this.storage = storage;
    }

    @Override
    public Set<String> getSlotsUriBy(String executionId) throws SQLException {
        var slotsUri = new HashSet<String>();

        DbOperation.execute(null, storage, con -> {
            try (var statement = con.prepareStatement(QUERY_GET_SLOTS_URI)) {
                statement.setString(1, executionId);
                ResultSet rs = statement.executeQuery();
                while (rs.next()) {
                    slotsUri.add(rs.getString("slot_uri"));
                }
            }
        });

        return slotsUri;
    }

    @Override
    public void putSlotsUriFor(String executionId, Collection<String> slotsUri) throws SQLException {
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
    public List<String> whichSlotsUriPresented(Collection<String> slotsUri) throws SQLException {
        var presentedSlotsUri = new ArrayList<String>();

        DbOperation.execute(null, storage, con -> {
            String slotsAsString = slotsUri.parallelStream().collect(Collectors.joining(", ", "(", ")"));

            try (var statement = con.prepareStatement(QUERY_FIND_PRESENTED_SLOTS_URI.formatted(slotsAsString))) {
                ResultSet rs = statement.executeQuery();
                while (rs.next()) {
                    presentedSlotsUri.add(rs.getString("slot_uri"));
                }
            }
        });

        return presentedSlotsUri;
    }
}
