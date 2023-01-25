package ai.lzy.allocator.alloc.dao.impl;

import ai.lzy.allocator.alloc.dao.VmDao;
import ai.lzy.allocator.model.Vm;
import ai.lzy.allocator.model.VolumeClaim;
import ai.lzy.allocator.model.VolumeRequest;
import ai.lzy.allocator.model.Workload;
import ai.lzy.allocator.storage.AllocatorDataSource;
import ai.lzy.allocator.vmpool.ClusterRegistry;
import ai.lzy.model.db.DbOperation;
import ai.lzy.model.db.Storage;
import ai.lzy.model.db.TransactionHandle;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.net.Inet6Address;
import java.net.UnknownHostException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import javax.inject.Named;

@Singleton
public class VmDaoImpl implements VmDao {
    private static final String SPEC_FIELDS =
        "id, session_id, pool_label, zone, init_workloads_json, workloads_json, volume_requests_json," +
        " v6_proxy_address, cluster_type";

    private static final String STATUS_FIELDS = "status";

    private static final String ALLOCATION_START_FIELDS =
        "allocation_op_id, allocation_started_at, allocation_deadline, owner_instance, vm_ott";

    private static final String ALLOCATION_FIELDS =
        ALLOCATION_START_FIELDS + ", vm_subject_id, tunnel_pod_name, allocator_meta_json, volume_claims_json";

    private static final String RUN_FIELDS =
        "vm_meta_json, last_activity_time, deadline";

    private static final String ALL_FIELDS =
        "%s, %s, %s, %s".formatted(SPEC_FIELDS, STATUS_FIELDS, ALLOCATION_FIELDS, RUN_FIELDS);

    private static final String QUERY_LOAD_NOT_COMPLETED_VMS = """
        SELECT %s
        FROM vm JOIN operation o ON vm.allocation_op_id = o.id
        WHERE o.done = FALSE AND vm.allocation_deadline > NOW() AND vm.owner_instance = ?
        """.formatted(Stream.of(ALL_FIELDS.split(",\s*")).map(x -> "vm." + x).collect(Collectors.joining(", ")));

    private static final String QUERY_CREATE_VM = """
        INSERT INTO vm (%s, %s, %s)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """.formatted(SPEC_FIELDS, STATUS_FIELDS, ALLOCATION_START_FIELDS);

    private static final String QUERY_UPDATE_VM_ACTIVITY = """
        UPDATE vm
        SET last_activity_time = ?
        WHERE id = ?""";

    private static final String QUERY_UPDATE_VM_DEADLINE = """
        UPDATE vm
        SET deadline = ?
        WHERE id = ?""";

    private static final String QUERY_LIST_SESSION_VMS = """
        SELECT %s
        FROM vm
        WHERE session_id = ?""".formatted(ALL_FIELDS);

    private static final String QUERY_DELETE_SESSION_VMS = """
        UPDATE vm
        SET status = 'DELETING'
        WHERE session_id = ?""";

    private static final String QUERY_LIST_ALIVE_VMS = """
        SELECT %s
        FROM vm""".formatted(ALL_FIELDS);

    private static final String QUERY_READ_VM = """
        SELECT %s
        FROM vm
        WHERE id = ?""".formatted(ALL_FIELDS);

    private static final String QUERY_LIST_VMS_TO_CLEANUP = """
        SELECT %s
        FROM vm
        WHERE (status = 'IDLE' AND deadline IS NOT NULL AND deadline < NOW())
           OR (status = 'DELETING')
           OR (status = 'ALLOCATING' AND allocation_deadline < NOW())
           OR (status = 'RUNNING' AND COALESCE(last_activity_time < NOW(), FALSE))
        LIMIT ?""".formatted(ALL_FIELDS);

    private static final String QUERY_MARK_FAILED_ALLOCATIONS = """
        UPDATE vm
        SET status = 'DELETING'
        WHERE status = 'ALLOCATING'
          AND allocation_op_id IN (SELECT id FROM operation WHERE done = TRUE AND error IS NOT NULL)
        RETURNING id""";

    private static final String QUERY_ACQUIRE_VM = """
        UPDATE vm
        SET status = 'RUNNING'
        WHERE
            session_id = ? AND pool_label = ? AND zone = ? AND status = 'IDLE'
            AND workloads_json = ? AND init_workloads_json = ?
            AND volume_requests_json = ?
            AND COALESCE(v6_proxy_address, '') = ?
        RETURNING %s
        """.formatted(ALL_FIELDS);

    private static final String QUERY_RELEASE_VM = """
        UPDATE vm
        SET status = 'IDLE', deadline = ?
        WHERE id = ?""";

    private static final String QUERY_SET_VM_RUNNING = """
        UPDATE vm
        SET status = 'RUNNING', vm_meta_json = ?, last_activity_time = ?
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
        SET volume_claims_json = ?
        WHERE id = ?""";

    private static final String QUERY_GET_VOLUME_CLAIMS = """
        SELECT volume_claims_json
        FROM vm
        WHERE id = ?""";

    private static final String QUERY_SET_VM_SUBJECT_ID = """
        UPDATE vm
        SET vm_subject_id = ?
        WHERE id = ?""";

    private static final String QUERY_SET_TUNNEL_PON_NAME = """
        UPDATE vm
        SET tunnel_pod_name = ?
        WHERE id = ?""";

    private static final String QUERY_DELETE_VM = """
        WITH vm_row AS (
            DELETE FROM vm
            WHERE id = ?
            RETURNING *
        )
        INSERT INTO dead_vms
        SELECT id, NOW() AS ts, JSONB_SET(ROW_TO_JSON("vm_row")::JSONB, '{status}', '"DEAD"') AS vm
        FROM vm_row""";

    private final Storage storage;
    private final ObjectMapper objectMapper;

    @Inject
    public VmDaoImpl(AllocatorDataSource storage, @Named("AllocatorObjectMapper") ObjectMapper objectMapper) {
        this.storage = storage;
        this.objectMapper = objectMapper;
    }

    @Override
    public Vm create(Vm.Spec vmSpec, String opId, Instant startedAt, Instant opDeadline, String vmOtt,
                     String allocatorId, @Nullable TransactionHandle transaction) throws SQLException
    {
        final var vmId = "vm-" + UUID.randomUUID();

        DbOperation.execute(transaction, storage, con -> {
            try (PreparedStatement s = con.prepareStatement(QUERY_CREATE_VM)) {
                // spec
                s.setString(1, vmId);
                s.setString(2, vmSpec.sessionId());
                s.setString(3, vmSpec.poolLabel());
                s.setString(4, vmSpec.zone());
                s.setString(5, objectMapper.writeValueAsString(vmSpec.initWorkloads()));
                s.setString(6, objectMapper.writeValueAsString(vmSpec.workloads()));
                s.setString(7, objectMapper.writeValueAsString(vmSpec.volumeRequests()));
                s.setString(8, vmSpec.proxyV6Address() == null ? null : vmSpec.proxyV6Address().getHostAddress());
                s.setString(9, vmSpec.clusterType().name());

                // status
                s.setString(10, Vm.Status.ALLOCATING.name());

                // allocation state
                s.setString(11, opId);
                s.setTimestamp(12, Timestamp.from(startedAt));
                s.setTimestamp(13, Timestamp.from(opDeadline));
                s.setString(14, allocatorId);
                s.setString(15, vmOtt);

                int ret = s.executeUpdate();
                assert ret == 1;
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Cannot dump values", e);
            }
        });

        return new Vm(vmSpec.withVmId(vmId), Vm.Status.ALLOCATING,
            new Vm.AllocateState(opId, startedAt, opDeadline, allocatorId, vmOtt));
    }

    @Override
    public void setStatus(String vmId, Vm.Status status, @Nullable TransactionHandle transaction)
        throws SQLException
    {
        DbOperation.execute(transaction, storage, con -> {
            try (PreparedStatement s = con.prepareStatement(QUERY_UPDATE_VM_STATUS)) {
                s.setString(1, status.name());
                s.setString(2, vmId);
                s.executeUpdate();
            }
        });
    }

    @Override
    public void setLastActivityTime(String vmId, Instant time) throws SQLException {
        try (var conn = storage.connect();
             var st = conn.prepareStatement(QUERY_UPDATE_VM_ACTIVITY))
        {
            st.setTimestamp(1, Timestamp.from(time));
            st.setString(2, vmId);
            st.executeUpdate();
        }
    }

    @Override
    public void setDeadline(String vmId, Instant time) throws SQLException {
        try (var conn = storage.connect();
             var st = conn.prepareStatement(QUERY_UPDATE_VM_DEADLINE))
        {
            st.setTimestamp(1, Timestamp.from(time));
            st.setString(2, vmId);
            st.executeUpdate();
        }
    }

    @Override
    public void deleteVm(String vmId, @jakarta.annotation.Nullable TransactionHandle transaction) throws SQLException {
        DbOperation.execute(transaction, storage, conn -> {
            try (PreparedStatement st = conn.prepareStatement(QUERY_DELETE_VM)) {
                st.setString(1, vmId);
                st.execute();
            }
        });
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
    public void delete(String sessionId, @Nullable TransactionHandle tx) throws SQLException {
        DbOperation.execute(tx, storage, conn -> {
            try (PreparedStatement st = conn.prepareStatement(QUERY_DELETE_SESSION_VMS)) {
                st.setString(1, sessionId);
                st.execute();
            }
        });
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
    public List<Vm> listVmsToClean(int limit) throws SQLException {
        try (var conn = storage.connect();
             var st = conn.prepareStatement(QUERY_LIST_VMS_TO_CLEANUP))
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
            try (PreparedStatement s = con.prepareStatement(QUERY_READ_VM)) {
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
    public Vm acquire(Vm.Spec vmSpec, @Nullable TransactionHandle transaction) throws SQLException {
        return DbOperation.execute(transaction, storage, con -> {
            try (PreparedStatement s = con.prepareStatement(QUERY_ACQUIRE_VM)) {
                s.setString(1, vmSpec.sessionId());
                s.setString(2, vmSpec.poolLabel());
                s.setString(3, vmSpec.zone());
                s.setString(4, objectMapper.writeValueAsString(vmSpec.workloads()));
                s.setString(5, objectMapper.writeValueAsString(vmSpec.initWorkloads()));
                s.setString(6, objectMapper.writeValueAsString(vmSpec.volumeRequests()));
                s.setString(7, vmSpec.proxyV6Address() != null ? vmSpec.proxyV6Address().getHostAddress() : "");

                final var res = s.executeQuery();
                if (!res.next()) {
                    return null;
                }

                return readVm(res);

            } catch (JsonProcessingException e) {
                throw new RuntimeException("Cannot dump values", e);
            }
        });
    }

    @Override
    public void release(String vmId, Instant deadline, @Nullable TransactionHandle transaction) throws SQLException {
        DbOperation.execute(transaction, storage, conn -> {
            try (PreparedStatement st = conn.prepareStatement(QUERY_RELEASE_VM)) {
                st.setTimestamp(1, Timestamp.from(deadline));
                st.setString(2, vmId);
                st.execute();
            }
        });
    }

    @Override
    public void setAllocatorMeta(String vmId, Map<String, String> meta, @Nullable TransactionHandle transaction)
        throws SQLException
    {
        DbOperation.execute(transaction, storage, con -> {
            try (PreparedStatement s = con.prepareStatement(QUERY_UPDATE_VM_ALLOCATION_META)) {
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
            try (PreparedStatement s = con.prepareStatement(QUERY_READ_VM_ALLOCATION_META)) {
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
                    meta.set(objectMapper.readValue(res.getString(1), new TypeReference<>() {}));
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
            try (PreparedStatement s = con.prepareStatement(QUERY_UPDATE_VOLUME_CLAIMS)) {
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
            try (PreparedStatement s = con.prepareStatement(QUERY_GET_VOLUME_CLAIMS)) {
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
                    volumeClaims.set(objectMapper.readValue(dumpedVolumeClaims, new TypeReference<>() {}));
                }
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Cannot read volume json from db", e);
            }
        });
        return volumeClaims.get();
    }

    @Override
    public void setVmSubjectId(String vmId, String vmSubjectId, @Nullable TransactionHandle transaction)
        throws SQLException
    {
        DbOperation.execute(transaction, storage, con -> {
            try (PreparedStatement s = con.prepareStatement(QUERY_SET_VM_SUBJECT_ID)) {
                s.setString(1, vmSubjectId);
                s.setString(2, vmId);
                s.executeUpdate();
            }
        });
    }

    @Override
    public void setTunnelPod(String vmId, String tunnelPodName, @Nullable TransactionHandle transaction)
        throws SQLException
    {
        DbOperation.execute(transaction, storage, con -> {
            try (PreparedStatement s = con.prepareStatement(QUERY_SET_TUNNEL_PON_NAME)) {
                s.setString(1, tunnelPodName);
                s.setString(2, vmId);
                s.executeUpdate();
            }
        });
    }

    @Override
    public List<Vm> loadAllocatingVms(String allocatorId, @Nullable TransactionHandle transaction)
        throws SQLException
    {
        final List<Vm> vms = new ArrayList<>();
        DbOperation.execute(transaction, storage, con -> {
            try (PreparedStatement s = con.prepareStatement(QUERY_LOAD_NOT_COMPLETED_VMS)) {
                s.setString(1, allocatorId);
                final var res = s.executeQuery();
                while (res.next()) {
                    var vm = readVm(res);
                    assert allocatorId.equals(vm.allocateState().ownerId());
                    vms.add(vm);
                }
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Cannot read vm", e);
            }
        });
        return vms;
    }

    @Override
    public List<String> findFailedVms(@Nullable TransactionHandle transaction)
        throws SQLException
    {
        return DbOperation.execute(transaction, storage, conn -> {
            try (PreparedStatement st = conn.prepareStatement(QUERY_MARK_FAILED_ALLOCATIONS)) {
                var rs = st.executeQuery();
                var ids = new ArrayList<String>();
                while (rs.next()) {
                    ids.add(rs.getString(1));
                }
                return ids;
            }
        });
    }

    @Override
    public void setVmRunning(String vmId, Map<String, String> vmMeta, Instant activityDeadline,
                             TransactionHandle transaction) throws SQLException
    {
        DbOperation.execute(transaction, storage, con -> {
            try (PreparedStatement st = con.prepareStatement(QUERY_SET_VM_RUNNING)) {
                st.setString(1, objectMapper.writeValueAsString(vmMeta));
                st.setTimestamp(2, Timestamp.from(activityDeadline));
                st.setString(3, vmId);
                st.executeUpdate();
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Cannot dump values", e);
            }
        });
    }

    private Vm readVm(ResultSet rs) throws SQLException, JsonProcessingException {
        int idx = 0;

        // spec
        final var id = rs.getString(++idx);
        final var sessionId = rs.getString(++idx);
        final var poolLabel = rs.getString(++idx);
        final var zone = rs.getString(++idx);
        final var initWorkloads = objectMapper.readValue(rs.getString(++idx), new TypeReference<List<Workload>>() {});
        final var workloads = objectMapper.readValue(rs.getString(++idx), new TypeReference<List<Workload>>() {});
        final var volumeRequests = objectMapper.readValue(rs.getString(++idx),
            new TypeReference<List<VolumeRequest>>() {});
        final var v6ProxyAddress = Optional.ofNullable(rs.getString(++idx))
            .map(x -> {
                try {
                    return (Inet6Address) Inet6Address.getByName(x);
                } catch (UnknownHostException e) {
                    throw new RuntimeException(e);
                }
            })
            .orElse(null);

        final var clusterType = ClusterRegistry.ClusterType.valueOf(rs.getString(++idx));

        // status
        final var vmStatus = Vm.Status.valueOf(rs.getString(++idx));

        // allocate state
        final var allocationOpId = rs.getString(++idx);
        final var allocationStartedAt = rs.getTimestamp(++idx).toInstant();
        final var allocationDeadline = rs.getTimestamp(++idx).toInstant();
        final var allocationOwner = rs.getString(++idx);
        final var vmOtt = rs.getString(++idx);
        final var vmSubjectId = rs.getString(++idx);
        final var tunnelPodName = rs.getString(++idx);
        final var allocatorMeta = Optional.ofNullable(rs.getString(++idx))
            .map(x -> {
                try {
                    return objectMapper.readValue(x, new TypeReference<LinkedHashMap<String, String>>() {});
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            })
            .orElse(null);
        final var volumeClaims = Optional.ofNullable(rs.getString(++idx))
            .map(x -> {
                try {
                    return objectMapper.readValue(x, new TypeReference<List<VolumeClaim>>() {});
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            })
            .orElse(null);

        // run state
        final var vmMeta = Optional.ofNullable(rs.getString(++idx))
            .map(x -> {
                try {
                    return objectMapper.readValue(x, new TypeReference<LinkedHashMap<String, String>>() {});
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            })
            .orElse(null);
        final var lastActivityTime = Optional.ofNullable(rs.getTimestamp(++idx)).map(Timestamp::toInstant).orElse(null);
        final var deadline = Optional.ofNullable(rs.getTimestamp(++idx)).map(Timestamp::toInstant).orElse(null);

        return new Vm(
            new Vm.Spec(id, sessionId, poolLabel, zone, initWorkloads, workloads,
                volumeRequests, v6ProxyAddress, clusterType),
            vmStatus,
            new Vm.AllocateState(allocationOpId, allocationStartedAt, allocationDeadline, allocationOwner, vmOtt,
                vmSubjectId, tunnelPodName, allocatorMeta, volumeClaims),
            vmMeta != null ? new Vm.RunState(vmMeta, lastActivityTime, deadline) : null
        );
    }
}
