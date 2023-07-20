package ai.lzy.service.dao.impl;

import ai.lzy.model.db.DbOperation;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.service.dao.DeleteAllocatorSessionOperationsDao;
import jakarta.annotation.Nullable;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@Singleton
public class DeleteAllocatorSessionOperationsDaoImpl implements DeleteAllocatorSessionOperationsDao {
    private static final Logger LOG = LogManager.getLogger(DeleteAllocatorSessionOperationsDaoImpl.class);

    private static final String QUERY_INSERT_OPERATION = """
        INSERT INTO delete_allocator_session_operations (op_id, service_instance_id, session_id)
        VALUES (?, ?, ?)""";

    private static final String QUERY_DELETE_EXEC_OPERATION = """
        DELETE FROM delete_allocator_session_operations
        WHERE op_id = ?""";

    public static final String QUERY_SET_ALLOC_OPERATION = """
        UPDATE delete_allocator_session_operations
        SET delete_session_op_id = ?
        WHERE op_id = ?""";

    private static final String QUERY_SELECT_UNCOMPLETED_OPERATIONS = """
        SELECT o.id as id,
               o.description as desc,
               o.idempotency_key as idk,
               state.session_id as sid,
               state.delete_session_op_id as allocOp
        FROM delete_allocator_session_operations state
        JOIN operation o ON state.op_id = o.id
        WHERE state.service_instance_id = ?""";


    private final LzyServiceStorage storage;

    public DeleteAllocatorSessionOperationsDaoImpl(LzyServiceStorage storage) {
        this.storage = storage;
    }

    @Override
    public void create(String opId, String sessionId, String instanceId, @Nullable TransactionHandle tx)
        throws SQLException
    {
        DbOperation.execute(tx, storage, connection -> {
            try (PreparedStatement st = connection.prepareStatement(QUERY_INSERT_OPERATION)) {
                st.setString(1, opId);
                st.setString(2, instanceId);
                st.setString(3, sessionId);
                st.executeUpdate();
            }
        });
    }

    @Override
    public void delete(String opId, @Nullable TransactionHandle tx) throws SQLException {
        DbOperation.execute(tx, storage, connection -> {
            try (PreparedStatement st = connection.prepareStatement(QUERY_DELETE_EXEC_OPERATION)) {
                st.setString(1, opId);
                if (st.executeUpdate() < 1) {
                    LOG.error("Cannot delete unknown operation: { opId: {} }", opId);
                    throw new RuntimeException("Operation with id='%s' not found".formatted(opId));
                }
            }
        });
    }

    @Override
    public void setAllocatorOperationId(String opId, String allocOpId, TransactionHandle tx) throws SQLException {
        DbOperation.execute(tx, storage, connection -> {
            try (PreparedStatement st = connection.prepareStatement(QUERY_SET_ALLOC_OPERATION)) {
                st.setString(1, allocOpId);
                st.setString(2, opId);
                if (st.executeUpdate() < 1) {
                    LOG.error("Cannot set allocator delete session operation: { opId: {}, allocOpId: {} }",
                        opId, allocOpId);
                    throw new RuntimeException("Operation with id='%s' not found".formatted(opId));
                }
            }
        });
    }

    @Override
    public List<OpState> list(String instanceId, @Nullable TransactionHandle tx) throws SQLException {
        return DbOperation.execute(tx, storage, connection -> {
            var result = new ArrayList<OpState>();
            try (PreparedStatement st = connection.prepareStatement(QUERY_SELECT_UNCOMPLETED_OPERATIONS)) {
                st.setString(1, instanceId);

                var rs = st.executeQuery();
                while (rs.next()) {
                    var opId = rs.getString("id");
                    var desc = rs.getString("desc");
                    var idk = rs.getString("idk");
                    var sid = rs.getString("sid");
                    var allocOpId = rs.getString("allocOp");

                    result.add(new OpState(opId, desc, idk, sid, allocOpId));
                }
            }

            return result;
        });
    }
}
