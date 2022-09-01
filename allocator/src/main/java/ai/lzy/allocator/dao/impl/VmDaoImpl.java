package ai.lzy.allocator.dao.impl;

import ai.lzy.allocator.dao.VmDao;
import ai.lzy.allocator.model.Vm;
import ai.lzy.allocator.model.Workload;
import ai.lzy.allocator.volume.VolumeClaim;
import ai.lzy.allocator.volume.VolumeRequest;
import ai.lzy.model.db.DbOperation;
import ai.lzy.model.db.Storage;
import ai.lzy.model.db.TransactionHandle;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

@Singleton
public class VmDaoImpl implements VmDao {
    private static final String SPEC_FIELDS =  " id, session_id, pool_label, \"zone\", " +
                                               " allocation_op_id, allocation_started_at," +
                                               " workloads_json, volume_requests_json ";
    private static final String STATE_FIELDS = " status, last_activity_time, deadline," +
                                               " allocation_deadline, vm_meta_json, volumes_json ";
    private static final String FIELDS = SPEC_FIELDS + ", " + STATE_FIELDS;

    private static final String QUERY_CREATE_VM = """
        INSERT INTO vm (%s)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""".formatted(SPEC_FIELDS + ", status");

    private static final String QUERY_UPDATE_VM = """
        UPDATE vm
        SET (%s) = (?, ?, ?, ?, ?, ?)
        WHERE id = ?""".formatted(STATE_FIELDS);

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
        SET status = 'DELETING'
        WHERE session_id = ?""";

    private static final String QUERY_LIST_ALIVE_VMS = """
        SELECT %s
        FROM vm
        WHERE status != 'DEAD'""".formatted(FIELDS);

    private static final String QUERY_READ_VM = """
        SELECT %s
        FROM vm
        WHERE id = ?""".formatted(FIELDS);

    private static final String QUERY_LIST_EXPIRED_VMS = """
        SELECT %s
        FROM vm
        WHERE (status = 'IDLE' AND deadline IS NOT NULL AND deadline < NOW())
           OR (status = 'DELETING')
           OR (status = 'CONNECTING' AND allocation_deadline IS NOT NULL AND allocation_deadline < NOW())
           OR (status != 'CREATED' AND status != 'DEAD' AND last_activity_time < NOW())
        LIMIT ?""".formatted(FIELDS);

    private static final String QUERY_ACQUIRE_VM = """
        SELECT %s
        FROM vm
        WHERE
            session_id = ? AND pool_label = ? AND zone = ? AND status = 'IDLE'
        LIMIT 1
        FOR UPDATE""".formatted(FIELDS);

    private static final String QUERY_RELEASE_VM = """
        UPDATE vm
        SET status = 'IDLE', deadline = ?
        WHERE id = ?""";

    private static final String QUERY_UPDATE_VM_ALLOCATION_META = """
        UPDATE vm
        SET allocator_meta_json = ?
        WHERE id = ?""";

    private static final String QUERY_READ_VM_ALLOCATION_META = """
        SELECT allocator_meta_json
        FROM vm
        WHERE id = ?""";

    private static final String QUERY_UPDATE_VM_STATUS = """
        UPDATE vm
        SET status = ?
        WHERE id = ?""";

    private static final String QUERY_UPDATE_VOLUME_CLAIMS = """
        UPDATE vm
        SET volumes_json = ?
        WHERE id = ?""";

    private static final String QUERY_GET_VOLUME_CLAIMS = """
        SELECT volume_claims
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
    public Vm.Spec create(String sessionId, String poolLabel, String zone, List<Workload> workload,
                          List<VolumeRequest> volumeRequests, String allocationOpId, Instant now,
                          @Nullable TransactionHandle transaction) throws SQLException
    {
        final var vmId = UUID.randomUUID().toString();
        final var vmSpec = new Vm.Spec(vmId, sessionId, now, poolLabel, zone, workload, volumeRequests);

        DbOperation.execute(transaction, storage, con -> {
            try (final var s = con.prepareStatement(QUERY_CREATE_VM)) {
                s.setString(1, vmSpec.vmId());
                s.setString(2, vmSpec.sessionId());
                s.setString(3, vmSpec.poolLabel());
                s.setString(4, vmSpec.zone());
                s.setString(5, allocationOpId);
                s.setTimestamp(6, Timestamp.from(now));
                s.setString(7, objectMapper.writeValueAsString(vmSpec.workloads()));
                s.setString(8, objectMapper.writeValueAsString(vmSpec.volumeRequests()));
                s.setString(9, Vm.VmStatus.CREATED.name());
                s.execute();
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Cannot dump values", e);
            }
        });
        return vmSpec;
    }

    @Override
    public void update(String vmId, Vm.State state, @Nullable TransactionHandle transaction) throws SQLException {
        DbOperation.execute(transaction, storage, con -> {
            try (final var s = con.prepareStatement(QUERY_UPDATE_VM)) {
                s.setString(1, state.status().name());
                s.setTimestamp(2, state.lastActivityTime() == null ? null : Timestamp.from(state.lastActivityTime()));
                s.setTimestamp(3, state.deadline() == null ? null : Timestamp.from(state.deadline()));
                s.setTimestamp(4, state.allocationDeadline() == null ? null :
                    Timestamp.from(state.allocationDeadline()));
                s.setString(5, objectMapper.writeValueAsString(state.vmMeta()));
                s.setString(6, objectMapper.writeValueAsString(state.volumeClaims()));
                s.setString(7, vmId);
                s.executeUpdate();
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Cannot dump values", e);
            }
        });
    }

    @Override
    public void updateStatus(String vmId, Vm.VmStatus status, @Nullable TransactionHandle transaction)
        throws SQLException
    {
        DbOperation.execute(transaction, storage, con -> {
            try (final var s = con.prepareStatement(QUERY_UPDATE_VM_STATUS)) {
                s.setString(1, status.name());
                s.setString(2, vmId);
                s.executeUpdate();
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
                throw new RuntimeException("Cannot read vm", e);
            }
        });
        return vm[0];
    }

    @Nullable
    @Override
    public Vm acquire(String sessionId, String poolId, String zone, @Nullable TransactionHandle outerTransaction)
        throws SQLException
    {
        final Vm[] vm = {null};
        try (final var transaction = TransactionHandle.getOrCreate(storage, outerTransaction)) {
            DbOperation.execute(transaction, storage, con -> {
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
                    vm[0] = new Vm(
                        vm[0].spec(),
                        new Vm.VmStateBuilder(vm[0].state())
                            .setStatus(Vm.VmStatus.RUNNING)
                            .build(),
                        vm[0].allocationOperationId()
                    );

                    res.updateString("status", Vm.VmStatus.RUNNING.name());
                    res.updateRow();

                } catch (JsonProcessingException e) {
                    throw new RuntimeException("Cannot dump values", e);
                }
            });

            transaction.commit();
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

    @Override
    public void setVolumeClaims(String vmId, List<VolumeClaim> volumeClaims,
                                @Nullable TransactionHandle transaction) throws SQLException
    {
        DbOperation.execute(transaction, storage, con -> {
            try (final var s = con.prepareStatement(QUERY_UPDATE_VOLUME_CLAIMS)) {
                s.setString(1, objectMapper.writeValueAsString(volumeClaims));
                s.setString(2, vmId);
                s.executeUpdate();
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Cannot dump values", e);
            }
        });
    }

    @Override
    public List<VolumeClaim> getVolumeClaims(String vmId, @Nullable TransactionHandle transaction) throws SQLException {
        final AtomicReference<List<VolumeClaim>> volumeClaims = new AtomicReference<>();
        DbOperation.execute(transaction, storage, con -> {
            try (final var s = con.prepareStatement(QUERY_GET_VOLUME_CLAIMS)) {
                s.setString(1, vmId);
                final var resultSet = s.executeQuery();
                if (!resultSet.next()) {
                    volumeClaims.set(null);
                    return;
                }
                final String dumpedVolumeClaims = resultSet.getString(1);
                if (dumpedVolumeClaims == null) {
                    volumeClaims.set(null);
                } else {
                    volumeClaims.set(objectMapper.readValue(dumpedVolumeClaims, new TypeReference<>() {
                    }));
                }
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Cannot read volume json from db", e);
            }
        });
        return volumeClaims.get();
    }

    private Vm readVm(ResultSet res) throws SQLException, JsonProcessingException {
        final var id = res.getString(1);
        final var sessionIdRes = res.getString(2);
        final var poolLabel = res.getString(3);
        final var zone = res.getString(4);
        final var allocationOpId = res.getString(5);
        final var allocationStartedAt = res.getTimestamp(6).toInstant();

        final var workloads = objectMapper.readValue(res.getString(7),
            new TypeReference<List<Workload>>() {});
        final var volumeRequests = objectMapper.readValue(
            res.getString(8), new TypeReference<List<VolumeRequest>>() {});

        final var vmStatus = Vm.VmStatus.valueOf(res.getString(9));
        final var lastActivityTimeTs = res.getTimestamp(10);
        final var lastActivityTime = lastActivityTimeTs == null ? null : lastActivityTimeTs.toInstant();

        final var deadlineTs = res.getTimestamp(11);
        final var deadline = deadlineTs == null ? null : deadlineTs.toInstant();

        final var allocationDeadlineTs = res.getTimestamp(12);
        final var allocationDeadline = allocationDeadlineTs == null ? null : allocationDeadlineTs.toInstant();

        final String vmMetaString = res.getString(13);
        final var vmMeta = vmMetaString == null ? null : objectMapper.readValue(vmMetaString,
            new TypeReference<Map<String, String>>() {});

        final String volumeClaimString = res.getString(14);
        final var volumeClaims = volumeClaimString == null ? null : objectMapper.readValue(volumeClaimString,
            new TypeReference<List<VolumeClaim>>() {});
        return new Vm(
            new Vm.Spec(id, sessionIdRes, allocationStartedAt, poolLabel, zone, workloads, volumeRequests),
            new Vm.State(vmStatus, lastActivityTime, deadline, allocationDeadline, vmMeta, volumeClaims),
            allocationOpId
        );
    }

    private static String forUpdate(@Nullable TransactionHandle tx) {
        return tx != null ? " FOR UPDATE" : "";
    }
}
