package ai.lzy.allocator.dao.impl;

import ai.lzy.allocator.dao.VmDao;
import ai.lzy.allocator.model.Vm;
import ai.lzy.allocator.model.Workload;
import ai.lzy.model.db.DbOperation;
import ai.lzy.model.db.Storage;
import ai.lzy.model.db.TransactionHandle;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import javax.annotation.Nullable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

@Singleton
public class VmDaoImpl implements VmDao {
    private static final String FIELDS = " id, session_id, pool_label, \"zone\", state," +
                                         " allocation_op_id, allocation_started_at," +
                                         " workloads_json, last_activity_time, deadline," +
                                         " allocation_deadline, vm_meta_json ";

    private static final String QUERY_CREATE_VM = """
        INSERT INTO vm (%s)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""".formatted(FIELDS);

    private static final String QUERY_UPDATE_VM = """
        UPDATE vm
        SET (%s) = (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        WHERE id = ?""".formatted(FIELDS);

    private static final String QUERY_UPDATE_VM_ACTIVITY = """
        UPDATE vm
        SET last_activity_time = ?
        WHERE id = ?""";

    private static final String QUERY_LIST_SESSION_VMS = """
        SELECT %s
        FROM vm
        WHERE session_id = ?""".formatted(FIELDS);

    private static final String QUERY_DELETE_SESSION_VMS = """
        UPDATE vm
        SET state = 'DELETING'
        WHERE session_id = ?""";

    private static final String QUERY_LIST_ALIVE_VMS = """
        SELECT %s
        FROM vm
        WHERE state != 'DEAD'""".formatted(FIELDS);

    private static final String QUERY_READ_VM = """
        SELECT %s
        FROM vm
        WHERE id = ?""".formatted(FIELDS);

    private static final String QUERY_LIST_EXPIRED_VMS = """
        SELECT %s
        FROM vm
        WHERE (state = 'IDLE' AND deadline IS NOT NULL AND deadline < NOW())
           OR (state = 'DELETING')
           OR (state = 'CONNECTING' AND allocation_deadline IS NOT NULL AND allocation_deadline < NOW())
           OR (state != 'CREATED' AND state != 'DEAD' AND last_activity_time < NOW())
        LIMIT ?""".formatted(FIELDS);

    private static final String QUERY_ACQUIRE_VM = """
        SELECT %s
        FROM vm
        WHERE
            session_id = ? AND pool_label = ? AND zone = ? AND state = 'IDLE'
        LIMIT 1
        FOR UPDATE""".formatted(FIELDS);

    private static final String QUERY_RELEASE_VM = """
        UPDATE vm
        SET state = 'IDLE', deadline = ?
        WHERE id = ?""";

    private static final String QUERY_UPDATE_VM_ALLOCATION_META = """
        UPDATE vm
        SET allocator_meta_json = ?
        WHERE id = ?""";

    private static final String QUERY_READ_VM_ALLOCATION_META = """
        SELECT allocator_meta_json
        FROM vm
        WHERE id = ?""";

    private final Storage storage;
    private final ObjectMapper objectMapper;

    @Inject
    public VmDaoImpl(AllocatorDataSource storage, ObjectMapper objectMapper) {
        this.storage = storage;
        this.objectMapper = objectMapper;
    }

    @Override
    public Vm create(String sessionId, String poolLabel, String zone, List<Workload> workload, String opId, Instant now,
                     @Nullable TransactionHandle transaction) throws SQLException
    {
        final var vmId = UUID.randomUUID().toString();
        final var vm = new Vm.VmBuilder(sessionId, vmId, poolLabel, zone, opId, now, workload, Vm.State.CREATED)
            .build();

        DbOperation.execute(transaction, storage, con -> {
            try (final var s = con.prepareStatement(QUERY_CREATE_VM)) {
                writeVm(s, vm);
                s.execute();
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Cannot dump values", e);
            }
        });
        return vm;
    }

    @Override
    public void update(Vm vm, @Nullable TransactionHandle transaction) throws SQLException {
        DbOperation.execute(transaction, storage, con -> {
            try (final var s = con.prepareStatement(QUERY_UPDATE_VM)) {
                writeVm(s, vm);
                s.setString(13, vm.vmId());
                s.executeUpdate();
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Cannot dump values", e);
            }
        });
    }

    @Override
    public void updateLastActivityTime(String vmId, Instant time) throws SQLException {
        try (var conn = storage.connect();
             var st = conn.prepareStatement(QUERY_UPDATE_VM_ACTIVITY))
        {
            st.setString(1, vmId);
            st.setTimestamp(2, Timestamp.from(time));
            st.executeUpdate();
        }
    }

    @Override
    public List<Vm> list(String sessionId) throws SQLException {
        try (var conn = storage.connect();
             var st = conn.prepareStatement(QUERY_LIST_SESSION_VMS))
        {
            st.setString(1, sessionId);
            final var res = st.executeQuery();

            final List<Vm> vms = new ArrayList<>();
            while (res.next()) {
                final Vm vm = readVm(res);
                vms.add(vm);
            }
            return vms;
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Cannot read vm", e);
        }
    }

    @Override
    public void delete(String sessionId) throws SQLException {
        try (var conn = storage.connect();
             var st = conn.prepareStatement(QUERY_DELETE_SESSION_VMS))
        {
            st.setString(1, sessionId);
            st.execute();
        }
    }

    @Override
    @VisibleForTesting
    public List<Vm> listAlive() throws SQLException {
        try (var conn = storage.connect();
             var st = conn.prepareStatement(QUERY_LIST_ALIVE_VMS))
        {
            final var res = st.executeQuery();
            final List<Vm> vms = new ArrayList<>();
            while (res.next()) {
                final Vm vm = readVm(res);
                vms.add(vm);
            }
            return vms;
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Cannot write vm", e);
        }
    }

    @Override
    public List<Vm> listExpired(int limit) throws SQLException {
        try (var conn = storage.connect();
             var st = conn.prepareStatement(QUERY_LIST_EXPIRED_VMS))
        {
            st.setInt(1, limit);
            final var res = st.executeQuery();

            final List<Vm> vms = new ArrayList<>();
            while (res.next()) {
                final Vm vm = readVm(res);
                vms.add(vm);
            }
            return vms;
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Cannot write vm", e);
        }
    }

    @Nullable
    @Override
    public Vm get(String vmId, TransactionHandle transaction) throws SQLException {
        final Vm[] vm = {null};
        DbOperation.execute(transaction, storage, con -> {
            try (final var s = con.prepareStatement(QUERY_READ_VM + forUpdate(transaction))) {
                s.setString(1, vmId);
                final var res = s.executeQuery();
                if (!res.next()) {
                    vm[0] = null;
                    return;
                }
                vm[0] = readVm(res);
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Cannot write vm", e);
            }
        });
        return vm[0];
    }

    @Nullable
    @Override
    public Vm acquire(String sessionId, String poolId, String zone, @Nullable TransactionHandle transaction)
        throws SQLException
    {
        final Vm[] vm = {null};
        try (final var tx = TransactionHandle.getOrCreate(storage, transaction)) {
            DbOperation.execute(tx, storage, con -> {
                try (final var s = con.prepareStatement(QUERY_ACQUIRE_VM,
                    ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE))
                {
                    s.setString(1, sessionId);
                    s.setString(2, poolId);
                    s.setString(3, zone);

                    final var res = s.executeQuery();
                    if (!res.next()) {
                        vm[0] = null;
                        return;
                    }

                    vm[0] = readVm(res);
                    vm[0] = Vm.from(vm[0])
                        .setState(Vm.State.RUNNING)
                        .build();

                    res.updateString("state", "RUNNING");
                    res.updateRow();

                } catch (JsonProcessingException e) {
                    throw new RuntimeException("Cannot dump values", e);
                }
            });
        }

        return vm[0];
    }

    @Override
    public void release(String vmId, Instant deadline, @Nullable TransactionHandle transaction) throws SQLException {
        DbOperation.execute(transaction, storage, conn -> {
            try (var st = conn.prepareStatement(QUERY_RELEASE_VM)) {
                st.setTimestamp(1, Timestamp.from(deadline));
                st.setString(2, vmId);
                st.execute();
            }
        });
    }

    @Override
    public void saveAllocatorMeta(String vmId, Map<String, String> meta, @Nullable TransactionHandle transaction)
        throws SQLException
    {
        DbOperation.execute(transaction, storage, con -> {
            try (final var s = con.prepareStatement(QUERY_UPDATE_VM_ALLOCATION_META)) {
                final ObjectMapper objectMapper = new ObjectMapper();
                s.setString(1, objectMapper.writeValueAsString(meta));
                s.setString(2, vmId);
                s.executeUpdate();
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Cannot dump values", e);
            }
        });
    }

    @Nullable
    @Override
    public Map<String, String> getAllocatorMeta(String vmId, @Nullable TransactionHandle transaction)
        throws SQLException
    {
        final AtomicReference<Map<String, String>> meta = new AtomicReference<>();
        DbOperation.execute(transaction, storage, con -> {
            try (final var s = con.prepareStatement(QUERY_READ_VM_ALLOCATION_META + forUpdate(transaction))) {
                final ObjectMapper objectMapper = new ObjectMapper();
                s.setString(1, vmId);
                final var res = s.executeQuery();
                if (!res.next()) {
                    meta.set(null);
                    return;
                }

                final var dumpedMeta = res.getString(1);
                if (dumpedMeta == null) {
                    meta.set(null);
                } else {
                    meta.set(objectMapper.readValue(res.getString(1), new TypeReference<>() {
                    }));
                }
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Cannot dump values", e);
            }
        });
        return meta.get();
    }

    private void writeVm(PreparedStatement s, Vm vm) throws SQLException, JsonProcessingException {
        s.setString(1, vm.vmId());
        s.setString(2, vm.sessionId());
        s.setString(3, vm.poolLabel());
        s.setString(4, vm.zone());
        s.setString(5, vm.state().name());
        s.setString(6, vm.allocationOperationId());
        s.setTimestamp(7, Timestamp.from(vm.allocationStartedAt()));
        s.setString(8, objectMapper.writeValueAsString(vm.workloads()));
        s.setTimestamp(9, vm.lastActivityTime() == null ? null : Timestamp.from(vm.lastActivityTime()));
        s.setTimestamp(10, vm.deadline() == null ? null : Timestamp.from(vm.deadline()));
        s.setTimestamp(11, vm.allocationDeadline() == null ? null : Timestamp.from(vm.allocationDeadline()));
        s.setString(12, objectMapper.writeValueAsString(vm.vmMeta()));
    }

    private Vm readVm(ResultSet res) throws SQLException, JsonProcessingException {
        final var id = res.getString(1);
        final var sessionIdRes = res.getString(2);
        final var poolLabel = res.getString(3);
        final var zone = res.getString(4);
        final var state = Vm.State.valueOf(res.getString(5));
        final var allocationOpId = res.getString(6);
        final var allocationStartedAt = res.getTimestamp(7).toInstant();
        final var workloads = objectMapper.readValue(res.getString(8), new TypeReference<List<Workload>>() {});

        final var lastActivityTimeTs = res.getTimestamp(9);
        final var lastActivityTime = lastActivityTimeTs == null ? null : lastActivityTimeTs.toInstant();

        final var deadlineTs = res.getTimestamp(10);
        final var deadline = deadlineTs == null ? null : deadlineTs.toInstant();

        final var allocationDeadlineTs = res.getTimestamp(11);
        final var allocationDeadline = allocationDeadlineTs == null ? null : allocationDeadlineTs.toInstant();

        final var vmMeta = objectMapper.readValue(res.getString(12), new TypeReference<Map<String, String>>() {});

        return new Vm(sessionIdRes, id, poolLabel, zone, state, allocationOpId, allocationStartedAt, workloads,
            lastActivityTime, deadline, allocationDeadline, vmMeta);
    }

    private static String forUpdate(@Nullable TransactionHandle tx) {
        return tx != null ? " FOR UPDATE" : "";
    }
}
