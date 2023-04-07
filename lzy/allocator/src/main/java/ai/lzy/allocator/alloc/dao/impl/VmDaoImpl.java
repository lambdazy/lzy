package ai.lzy.allocator.alloc.dao.impl;

import ai.lzy.allocator.alloc.dao.VmDao;
import ai.lzy.allocator.model.Vm;
import ai.lzy.allocator.model.VolumeClaim;
import ai.lzy.allocator.model.VolumeRequest;
import ai.lzy.allocator.model.Workload;
import ai.lzy.allocator.storage.AllocatorDataSource;
import ai.lzy.allocator.util.DaoUtils;
import ai.lzy.allocator.vmpool.ClusterRegistry;
import ai.lzy.common.IdGenerator;
import ai.lzy.model.db.DbOperation;
import ai.lzy.model.db.Storage;
import ai.lzy.model.db.TransactionHandle;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import jakarta.annotation.Nullable;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;

import java.net.Inet6Address;
import java.net.UnknownHostException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.*;

import static java.util.Arrays.stream;
import static java.util.stream.Collectors.joining;

@Singleton
public class VmDaoImpl implements VmDao {
    private static final String SPEC_FIELDS =
        "id, session_id, pool_label, zone, init_workloads_json, workloads_json, " +
        "volume_requests_json, volume_descriptions_json, v6_proxy_address, tunnel_index, cluster_type";

    private static final String STATUS_FIELDS =
        "status";

    private static final String INSTANCE_FIELDS =
        "tunnel_pod_name, mount_pod_name";

    private static final String ALLOCATION_START_FIELDS =
        "allocation_op_id, allocation_started_at, allocation_deadline, allocation_worker, allocation_reqid, vm_ott";

    private static final String ALLOCATION_FIELDS =
        ALLOCATION_START_FIELDS + ", allocator_meta_json, volume_claims_json";

    private static final String RUN_FIELDS =
        "vm_meta_json, activity_deadline";

    private static final String IDLE_FIELDS =
        "idle_since, idle_deadline";

    private static final String DELETE_FIELDS =
        "delete_op_id, delete_worker, delete_reqid";

    private static final String ALL_FIELDS =
        "%s, %s, %s, %s, %s, %s, %s".formatted(
            SPEC_FIELDS, STATUS_FIELDS, INSTANCE_FIELDS, ALLOCATION_FIELDS, RUN_FIELDS, IDLE_FIELDS, DELETE_FIELDS);

    /*** -= QUERIES =- ***/

    private static final String QUERY_READ_VM = """
        SELECT %s
        FROM vm
        WHERE id = ?""".formatted(ALL_FIELDS);

    private static final String QUERY_READ_SESSION_VMS = """
        SELECT %s
        FROM vm
        WHERE session_id = ?""".formatted(ALL_FIELDS);

    private static final String QUERY_CREATE_VM = """
        INSERT INTO vm (%s, %s, %s)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """.formatted(SPEC_FIELDS, STATUS_FIELDS, ALLOCATION_START_FIELDS);

    private static final String QUERY_START_DELETE_VM = """
        UPDATE vm
        SET status = 'DELETING', delete_op_id = ?, delete_worker = ?, delete_reqid = ?
        WHERE id = ?""";

    private static final String QUERY_CLEANUP_VM = """
        WITH vm_row AS (
            DELETE FROM vm
            WHERE id = ?
            RETURNING *
        )
        INSERT INTO dead_vms
        SELECT id, NOW() AS ts, JSONB_SET(ROW_TO_JSON("vm_row")::JSONB, '{status}', '"DEAD"') AS vm
        FROM vm_row""";

    private static final String QUERY_ACQUIRE_VM = """
        WITH existing_vm AS (
            SELECT %s
            FROM vm
            WHERE session_id = ? AND pool_label = ? AND zone = ? AND status = 'IDLE'
                AND workloads_json = ? AND init_workloads_json = ?
                AND volume_descriptions_json = ?
                AND COALESCE(v6_proxy_address, '') = ?
            LIMIT 1
        )
        UPDATE vm
        SET status = 'RUNNING', idle_since = NULL, idle_deadline = NULL
        WHERE id = (SELECT id FROM existing_vm)
        RETURNING
            %s,
            (SELECT idle_since FROM existing_vm) AS was_idle_since,
            (SELECT idle_deadline FROM existing_vm) AS was_idle_deadline
        """.formatted(ALL_FIELDS, stream(ALL_FIELDS.split(",")).map(s -> "vm." + s.trim()).collect(joining(", ")));

    private static final String QUERY_RELEASE_VM = """
        UPDATE vm
        SET status = 'IDLE', idle_since = NOW(), idle_deadline = ?
        WHERE id = ? AND status = 'RUNNING'""";

    private static final String QUERY_COUNT_CACHED_VMS = """
        SELECT session_id, pool_label, COUNT(*) AS cnt
        FROM vm
        WHERE status = 'IDLE'
          AND session_id IN (SELECT id FROM session WHERE owner = ? AND session.delete_op_id IS NULL)
        GROUP BY session_id, pool_label
        """;

    private static final String QUERY_UPDATE_VM_ALLOCATION_META = """
        UPDATE vm
        SET allocator_meta_json = ?
        WHERE id = ?""";

    private static final String QUERY_READ_VM_ALLOCATION_META = """
        SELECT allocator_meta_json
        FROM vm
        WHERE id = ?""";

    private static final String QUERY_SET_TUNNEL_PON_NAME = """
        UPDATE vm
        SET tunnel_pod_name = ?
        WHERE id = ?""";

    private static final String QUERY_SET_MOUNT_PON_NAME = """
        UPDATE vm
        SET mount_pod_name = ?
        WHERE id = ?""";

    private static final String QUERY_UPDATE_VOLUME_CLAIMS = """
        UPDATE vm
        SET volume_claims_json = ?
        WHERE id = ?""";

    private static final String QUERY_GET_VOLUME_CLAIMS = """
        SELECT volume_claims_json
        FROM vm
        WHERE id = ?""";

    private static final String QUERY_SET_VM_RUNNING = """
        UPDATE vm
        SET status = 'RUNNING', idle_since = NULL, idle_deadline = null, vm_meta_json = ?, activity_deadline = ?
        WHERE id = ?""";

    private static final String QUERY_UPDATE_VM_ACTIVITY = """
        UPDATE vm
        SET activity_deadline = ?
        WHERE id = ?""";

    private static final String QUERY_LIST_ALIVE_VMS = """
        SELECT %s
        FROM vm""".formatted(ALL_FIELDS);

    private static final String QUERY_LIST_EXPIRED_VMS = """
        SELECT %s
        FROM vm
        WHERE (status = 'IDLE' AND idle_deadline < NOW())
           OR ((status = 'RUNNING' OR status = 'IDLE') AND activity_deadline < NOW())
        LIMIT ?""".formatted(ALL_FIELDS);

    private static final String QUERY_LOAD_NOT_COMPLETED_VMS = """
        SELECT %s
        FROM vm
        WHERE (status = 'ALLOCATING' AND allocation_worker = ?)
           OR (status = 'DELETING' AND delete_worker = ?)
        """.formatted(ALL_FIELDS);

    private static final String QUERY_LOAD_IDLE_VMS = """
        SELECT %s
        FROM vm
        WHERE (status = 'IDLE' OR status = 'RUNNING') AND allocation_worker = ?
        """.formatted(ALL_FIELDS);

    private static final String QUERY_READ_VM_BY_OTT = """
        SELECT %s
        FROM vm
        WHERE vm_ott = ?""".formatted(ALL_FIELDS);

    private static final String QUERY_RESET_VM_OTT = """
        UPDATE vm
        SET vm_ott = ''
        WHERE vm_ott = ?
        RETURNING id""";

    private static final String QUERY_VM_BY_IDS_TEMPLATE = """
        SELECT %s
        FROM vm
        WHERE id in %s
        """;


    private final Storage storage;
    private final ObjectMapper objectMapper;
    private final IdGenerator idGenerator;

    @Inject
    public VmDaoImpl(AllocatorDataSource storage, @Named("AllocatorObjectMapper") ObjectMapper objectMapper,
                     @Named("AllocatorIdGenerator") IdGenerator idGenerator)
    {
        this.storage = storage;
        this.objectMapper = objectMapper;
        this.idGenerator = idGenerator;
    }

    @Nullable
    @Override
    public Vm get(String vmId, @Nullable TransactionHandle tx) throws SQLException {
        return DbOperation.execute(tx, storage, con -> {
            try (PreparedStatement s = con.prepareStatement(QUERY_READ_VM)) {
                s.setString(1, vmId);
                final var res = s.executeQuery();
                if (res.next()) {
                    return readVm(res);
                }
                return null;
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Cannot read vm", e);
            }
        });
    }

    @Override
    public List<Vm> getSessionVms(String sessionId, TransactionHandle tx) throws SQLException {
        return DbOperation.execute(tx, storage, conn -> {
            try (PreparedStatement st = conn.prepareStatement(QUERY_READ_SESSION_VMS)) {
                st.setString(1, sessionId);
                final var res = st.executeQuery();

                final List<Vm> vms = new ArrayList<>();
                while (res.next()) {
                    vms.add(readVm(res));
                }
                return vms;
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Cannot read vm", e);
            }
        });
    }

    @Override
    public Vm create(Vm.Spec vmSpec, Vm.AllocateState allocState, @Nullable TransactionHandle tx) throws SQLException {
        final var vmId = idGenerator.generate("vm-" + vmSpec.poolLabel() + "-", 16);

        DbOperation.execute(tx, storage, con -> {
            try (PreparedStatement s = con.prepareStatement(QUERY_CREATE_VM)) {
                var initWorkloads = vmSpec.initWorkloads().stream()
                    .sorted(Comparator.comparing(Workload::image))
                    .toList();
                var workloads = vmSpec.workloads().stream()
                    .sorted(Comparator.comparing(Workload::image))
                    .toList();
                var volumeRequests = vmSpec.volumeRequests().stream()
                    .sorted(Comparator.comparing(r -> r.volumeDescription().name()))
                    .toList();
                var volumeDescriptions = volumeRequests.stream()
                    .map(VolumeRequest::volumeDescription)
                    .toList();

                int idx = 0;

                // spec
                s.setString(++idx, vmId);
                s.setString(++idx, vmSpec.sessionId());
                s.setString(++idx, vmSpec.poolLabel());
                s.setString(++idx, vmSpec.zone());
                s.setString(++idx, objectMapper.writeValueAsString(initWorkloads));
                s.setString(++idx, objectMapper.writeValueAsString(workloads));
                s.setString(++idx, objectMapper.writeValueAsString(volumeRequests));
                s.setString(++idx, objectMapper.writeValueAsString(volumeDescriptions));
                s.setString(++idx, vmSpec.tunnelSettings() == null ? null
                    : vmSpec.tunnelSettings().proxyV6Address().getHostAddress());
                if (vmSpec.tunnelSettings() == null) {
                    s.setNull(++idx, Types.INTEGER);
                } else {
                    s.setInt(++idx, vmSpec.tunnelSettings().tunnelIndex());
                }
                s.setString(++idx, vmSpec.clusterType().name());

                // status
                s.setString(++idx, Vm.Status.ALLOCATING.name());

                // allocation state
                s.setString(++idx, allocState.operationId());
                s.setTimestamp(++idx, Timestamp.from(allocState.startedAt()));
                s.setTimestamp(++idx, Timestamp.from(allocState.deadline()));
                s.setString(++idx, allocState.worker());
                s.setString(++idx, allocState.reqid());
                s.setString(++idx, allocState.vmOtt());

                int ret = s.executeUpdate();
                assert ret == 1;
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Cannot dump values", e);
            }
        });

        return new Vm(vmSpec.withVmId(vmId), Vm.Status.ALLOCATING, allocState);
    }

    @Override
    public void delete(String vmId, Vm.DeletingState deleteState, @Nullable TransactionHandle tx) throws SQLException {
        DbOperation.execute(tx, storage, conn -> {
            try (PreparedStatement st = conn.prepareStatement(QUERY_START_DELETE_VM)) {
                int idx = 0;
                st.setString(++idx, deleteState.operationId());
                st.setString(++idx, deleteState.worker());
                st.setString(++idx, deleteState.reqid());
                st.setString(++idx, vmId);
                var updated = st.executeUpdate();
                if (updated != 1) {
                    throw new RuntimeException("Cannot start deleting of VM " + vmId);
                }
            }
        });
    }

    @Override
    public void cleanupVm(String vmId, TransactionHandle tx) throws SQLException {
        DbOperation.execute(tx, storage, conn -> {
            try (PreparedStatement st = conn.prepareStatement(QUERY_CLEANUP_VM)) {
                st.setString(1, vmId);
                st.execute();
            }
        });
    }

    @Nullable
    @Override
    public Vm acquire(Vm.Spec vmSpec, @Nullable TransactionHandle tx) throws SQLException {
        return DbOperation.execute(tx, storage, con -> {
            try (PreparedStatement s = con.prepareStatement(QUERY_ACQUIRE_VM)) {

                List<Workload> workloads = vmSpec.workloads().stream()
                    .sorted(Comparator.comparing(Workload::image))
                    .toList();

                List<Workload> initWorkloads = vmSpec.initWorkloads().stream()
                    .sorted(Comparator.comparing(Workload::image))
                    .toList();

                List<VolumeRequest.VolumeDescription> volumeDescriptions = vmSpec.volumeRequests().stream()
                    .sorted(Comparator.comparing(r -> r.volumeDescription().name()))
                    .map(VolumeRequest::volumeDescription)
                    .toList();

                int idx = 0;
                s.setString(++idx, vmSpec.sessionId());
                s.setString(++idx, vmSpec.poolLabel());
                s.setString(++idx, vmSpec.zone());
                s.setString(++idx, objectMapper.writeValueAsString(workloads));
                s.setString(++idx, objectMapper.writeValueAsString(initWorkloads));
                s.setString(++idx, objectMapper.writeValueAsString(volumeDescriptions));
                s.setString(++idx, vmSpec.tunnelSettings() != null
                    ? vmSpec.tunnelSettings().proxyV6Address().getHostAddress() : "");

                final var res = s.executeQuery();
                if (!res.next()) {
                    return null;
                }

                var vm = readVm(res);

                return new Vm(
                    vm.spec(),
                    Vm.Status.IDLE,
                    vm.instanceProperties(),
                    vm.allocateState(),
                    vm.runState(),
                    new Vm.IdleState(
                        res.getTimestamp("was_idle_since").toInstant(),
                        res.getTimestamp("was_idle_deadline").toInstant()),
                    null);
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Cannot dump values", e);
            }
        });
    }

    @Override
    public void release(String vmId, Instant deadline, @Nullable TransactionHandle tx) throws SQLException {
        DbOperation.execute(tx, storage, conn -> {
            try (PreparedStatement st = conn.prepareStatement(QUERY_RELEASE_VM)) {
                st.setTimestamp(1, Timestamp.from(deadline));
                st.setString(2, vmId);
                int ret = st.executeUpdate();
                if (ret != 1) {
                    throw new RuntimeException("Cannot release VM %s".formatted(vmId));
                }
            }
        });
    }

    @Override
    public CachedVms countCachedVms(Vm.Spec vmSpec, String owner, @Nullable TransactionHandle tx) throws SQLException {
        return DbOperation.execute(tx, storage, conn -> {
            try (PreparedStatement st = conn.prepareStatement(QUERY_COUNT_CACHED_VMS)) {
                st.setString(1, owner);
                var rs = st.executeQuery();

                int atPoolAndSession = 0;
                int atSession = 0;
                int atOwner = 0;

                while (rs.next()) {
                    var sessionId = rs.getString(1);
                    var poolLabel = rs.getString(2);
                    var count = rs.getInt(3);

                    if (vmSpec.sessionId().equals(sessionId)) {
                        atSession += count;
                        if (vmSpec.poolLabel().equals(poolLabel)) {
                            atPoolAndSession += count;
                        }
                    }

                    atOwner += count;
                }
                return new CachedVms(atPoolAndSession, atSession, atOwner);
            }
        });
    }

    @Override
    public void setAllocatorMeta(String vmId, Map<String, String> meta, @Nullable TransactionHandle tx)
        throws SQLException
    {
        DbOperation.execute(tx, storage, con -> {
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
    public Map<String, String> getAllocatorMeta(String vmId, @Nullable TransactionHandle tx) throws SQLException {
        return DbOperation.execute(tx, storage, con -> {
            try (PreparedStatement s = con.prepareStatement(QUERY_READ_VM_ALLOCATION_META)) {
                s.setString(1, vmId);
                final var res = s.executeQuery();
                if (!res.next()) {
                    return null;
                }

                final var dumpedMeta = res.getString(1);
                if (dumpedMeta == null) {
                    return null;
                } else {
                    return objectMapper.readValue(res.getString(1), new TypeReference<>() {});
                }
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Cannot dump values", e);
            }
        });
    }

    @Override
    public void setTunnelPod(String vmId, String tunnelPodName, @Nullable TransactionHandle tx) throws SQLException {
        DbOperation.execute(tx, storage, con -> {
            try (PreparedStatement s = con.prepareStatement(QUERY_SET_TUNNEL_PON_NAME)) {
                s.setString(1, tunnelPodName);
                s.setString(2, vmId);
                s.executeUpdate();
            }
        });
    }

    @Override
    public void setMountPod(String vmId, String mountPodName, TransactionHandle tx) throws SQLException {
        DbOperation.execute(tx, storage, con -> {
            try (PreparedStatement s = con.prepareStatement(QUERY_SET_MOUNT_PON_NAME)) {
                s.setString(1, mountPodName);
                s.setString(2, vmId);
                s.executeUpdate();
            }
        });
    }

    @Override
    public void setVolumeClaims(String vmId, List<VolumeClaim> volumeClaims, @Nullable TransactionHandle tx)
        throws SQLException
    {
        DbOperation.execute(tx, storage, con -> {
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
    public List<VolumeClaim> getVolumeClaims(String vmId, @Nullable TransactionHandle tx) throws SQLException {
        return DbOperation.execute(tx, storage, con -> {
            try (PreparedStatement s = con.prepareStatement(QUERY_GET_VOLUME_CLAIMS)) {
                s.setString(1, vmId);
                final var resultSet = s.executeQuery();
                if (!resultSet.next()) {
                    return null;
                }

                final String dumpedVolumeClaims = resultSet.getString(1);
                if (dumpedVolumeClaims == null) {
                    return null;
                } else {
                    return objectMapper.readValue(dumpedVolumeClaims, new TypeReference<>() {});
                }
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Cannot read volume json from db", e);
            }
        });
    }

    @Override
    public void setVmRunning(String vmId, Map<String, String> vmMeta, Instant activityDeadline, TransactionHandle tx)
        throws SQLException
    {
        DbOperation.execute(tx, storage, con -> {
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

    @Override
    public void updateActivityDeadline(String vmId, Instant deadline) throws SQLException {
        try (var conn = storage.connect();
             var st = conn.prepareStatement(QUERY_UPDATE_VM_ACTIVITY))
        {
            st.setTimestamp(1, Timestamp.from(deadline));
            st.setString(2, vmId);
            st.executeUpdate();
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
    public List<Vm> listExpiredVms(int limit) throws SQLException {
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

    @Override
    public List<Vm> loadActiveVmsActions(String workerId, @Nullable TransactionHandle tx) throws SQLException {
        return DbOperation.execute(tx, storage, con -> {
            try (PreparedStatement s = con.prepareStatement(QUERY_LOAD_NOT_COMPLETED_VMS)) {
                s.setString(1, workerId);
                s.setString(2, workerId);
                final var res = s.executeQuery();
                final var vms = new ArrayList<Vm>();
                while (res.next()) {
                    vms.add(readVm(res));
                }
                return vms;
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Cannot read vm", e);
            }
        });
    }

    @Override
    public List<Vm> loadRunningVms(String workerId, @Nullable TransactionHandle tx) throws SQLException {
        return DbOperation.execute(tx, storage, conn -> {
            try (PreparedStatement s = conn.prepareStatement(QUERY_LOAD_IDLE_VMS)) {
                s.setString(1, workerId);
                final var res = s.executeQuery();
                final var vms = new ArrayList<Vm>();
                while (res.next()) {
                    vms.add(readVm(res));
                }
                return vms;
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Cannot read vm", e);
            }
        });
    }

    @Override
    public boolean hasDeadVm(String vmId) throws SQLException {
        try (var conn = storage.connect();
             PreparedStatement st = conn.prepareStatement("""
                SELECT 1
                FROM dead_vms
                WHERE id = ?"""))
        {
            st.setString(1, vmId);
            var rs = st.executeQuery();
            return rs.next();
        }
    }

    @Nullable
    @Override
    public Vm findVmByOtt(String vmOtt, @Nullable TransactionHandle tx) throws SQLException {
        return DbOperation.execute(tx, storage, conn -> {
            try (PreparedStatement st = conn.prepareStatement(QUERY_READ_VM_BY_OTT)) {
                st.setString(1, vmOtt);
                var rs = st.executeQuery();
                if (rs.next()) {
                    try {
                        return readVm(rs);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException("Cannot read vm by ott " + vmOtt, e);
                    }
                } else {
                    return null;
                }
            }
        });
    }

    @Override
    public String resetVmOtt(String vmOtt, @Nullable TransactionHandle tx) throws SQLException {
        return DbOperation.execute(tx, storage, conn -> {
            try (PreparedStatement st = conn.prepareStatement(QUERY_RESET_VM_OTT)) {
                st.setString(1, vmOtt);
                var rs = st.executeQuery();
                return rs.next() ? rs.getString("id") : null;
            }
        });
    }

    @Override
    public List<Vm> loadByIds(Set<String> vmIds, TransactionHandle tx) throws SQLException {
        if (vmIds.isEmpty()) {
            return List.of();
        }
        return DbOperation.execute(tx, storage, conn -> {
            final var vms = new ArrayList<Vm>(vmIds.size());
            for (List<String> idsPart : Lists.partition(List.copyOf(vmIds), 100)) {
                var params = DaoUtils.generateNParamArray(idsPart.size());
                var query = QUERY_VM_BY_IDS_TEMPLATE.formatted(ALL_FIELDS, params);
                try (PreparedStatement s = conn.prepareStatement(query)) {
                    int i = 1;

                    for (String vmId : idsPart) {
                        s.setString(i++, vmId);
                    }

                    final var res = s.executeQuery();
                    while (res.next()) {
                        vms.add(readVm(res));
                    }
                } catch (JsonProcessingException e) {
                    throw new RuntimeException("Cannot read vm", e);
                }
            }
            return vms;
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
        ++idx; // skip descriptions
        final var v6ProxyAddress = Optional.ofNullable(rs.getString(++idx))
            .map(x -> {
                try {
                    return (Inet6Address) Inet6Address.getByName(x);
                } catch (UnknownHostException e) {
                    throw new RuntimeException(e);
                }
            })
            .orElse(null);
        Integer tunnelIndex = rs.getInt(++idx);
        if (rs.wasNull()) {
            tunnelIndex = null;
        }

        final var clusterType = ClusterRegistry.ClusterType.valueOf(rs.getString(++idx));

        // status
        final var vmStatus = Vm.Status.valueOf(rs.getString(++idx));

        // instance properties
        final var tunnelPodName = rs.getString(++idx);
        final var mountPodName = rs.getString(++idx);

        // allocate state
        final var allocationOpId = rs.getString(++idx);
        final var allocationStartedAt = rs.getTimestamp(++idx).toInstant();
        final var allocationDeadline = rs.getTimestamp(++idx).toInstant();
        final var allocationWorker = rs.getString(++idx);
        final var allocationReqid = rs.getString(++idx);
        final var vmOtt = rs.getString(++idx);
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
        final Vm.RunState runState;
        if (vmStatus == Vm.Status.RUNNING || vmStatus == Vm.Status.IDLE) {
            final LinkedHashMap<String, String> vmMeta;
            try {
                vmMeta = objectMapper.readValue(rs.getString(++idx), new TypeReference<>() {});
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
            final var lastActivityTime = rs.getTimestamp(++idx).toInstant();
            runState = new Vm.RunState(vmMeta, lastActivityTime);
        } else {
            idx += 2;
            runState = null;
        }

        // idle state
        final Vm.IdleState idleState;
        if (vmStatus == Vm.Status.IDLE) {
            final var idleSice = rs.getTimestamp(++idx).toInstant();
            final var idleDeadline = rs.getTimestamp(++idx).toInstant();
            idleState = new Vm.IdleState(idleSice, idleDeadline);
        } else {
            idx += 2;
            idleState = null;
        }

        // deleting state
        final Vm.DeletingState deleteState;
        if (vmStatus == Vm.Status.DELETING) {
            final var deleteOperationId = rs.getString(++idx);
            final var deleteWorker = rs.getString(++idx);
            final var deleteReqid = rs.getString(++idx);
            deleteState = new Vm.DeletingState(deleteOperationId, deleteWorker, deleteReqid);
        } else {
            idx += 3;
            deleteState = null;
        }

        final Vm.TunnelSettings tunnelSettings;
        if (v6ProxyAddress != null && tunnelIndex != null) {
            tunnelSettings = new Vm.TunnelSettings(v6ProxyAddress, tunnelIndex);
        } else {
            tunnelSettings = null;
        }

        return new Vm(
            new Vm.Spec(id, sessionId, poolLabel, zone, initWorkloads, workloads, volumeRequests, tunnelSettings,
                clusterType),
            vmStatus,
            new Vm.InstanceProperties(tunnelPodName, mountPodName),
            new Vm.AllocateState(allocationOpId, allocationStartedAt, allocationDeadline, allocationWorker,
                allocationReqid, vmOtt, allocatorMeta, volumeClaims),
            runState,
            idleState,
            deleteState
        );
    }
}
