package ai.lzy.allocator.dao.impl;

import ai.lzy.allocator.dao.VmDao;
import ai.lzy.allocator.model.Vm;
import ai.lzy.allocator.model.Workload;
import ai.lzy.model.db.DbOperation;
import ai.lzy.model.db.Storage;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.TransactionHandleImpl;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import javax.annotation.Nullable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

@Singleton
public class VmDaoImpl implements VmDao {
    private static final String FIELDS = " id, session_id, pool_label, \"zone\", state, allocation_op_id,"
        + " workloads_json, last_activity_time, deadline, allocation_deadline, vm_meta_json ";
    private final Storage storage;
    private final ObjectMapper objectMapper;

    @Inject
    public VmDaoImpl(AllocatorDataSource storage, ObjectMapper objectMapper) {
        this.storage = storage;
        this.objectMapper = objectMapper;
    }

    @Override
    public Vm create(String sessionId, String poolLabel, String zone, List<Workload> workload, String opId,
                     @Nullable TransactionHandle transaction) {
        final var vm = new Vm.VmBuilder(sessionId, UUID.randomUUID().toString(), poolLabel, zone, opId, workload,
            Vm.State.CREATED).build();
        DbOperation.execute(transaction, storage, con -> {
            try (final var s = con.prepareStatement(
                "INSERT INTO vm ("
                    + FIELDS + """
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """)) {
                writeVm(s, vm);
                s.execute();
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Cannot dump values", e);
            }
        });
        return vm;
    }

    @Override
    public void update(Vm vm, @Nullable TransactionHandle transaction) {
        DbOperation.execute(transaction, storage, con -> {
            try (final var s = con.prepareStatement(
                "UPDATE vm SET ("
                    + FIELDS + """
                    ) = (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    WHERE id = ?
                    """)) {
                writeVm(s, vm);
                s.setString(12, vm.vmId());
                s.executeUpdate();
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Cannot dump values", e);
            }
        });
    }

    @Override
    public List<Vm> list(String sessionId, @Nullable TransactionHandle transaction) {
        final List<Vm> vms = new ArrayList<>();
        DbOperation.execute(transaction, storage, con -> {
            try (final var s = con.prepareStatement(
                "SELECT " + FIELDS + """
                     FROM vm
                     WHERE session_id = ?
                    """)) {
                s.setString(1, sessionId);
                final var res = s.executeQuery();
                while (res.next()) {
                    final Vm vm = readVm(res);
                    vms.add(vm);
                }
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Cannot write vm", e);
            }
        });
        return vms;
    }

    @Nullable
    @Override
    public Vm get(String vmId, TransactionHandle transaction) {
        final Vm[] vm = new Vm[1];
        DbOperation.execute(transaction, storage, con -> {
            try (final var s = con.prepareStatement(
                "SELECT " + FIELDS + """
                     FROM vm
                     WHERE id = ?
                    """)) {
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

    @Override
    public List<Vm> getExpired(int limit, @Nullable TransactionHandle transaction) {
        final List<Vm> vms = new ArrayList<>();
        DbOperation.execute(transaction, storage, con -> {
            try (final var s = con.prepareStatement(
                "SELECT " + FIELDS + """
                     FROM vm
                     WHERE (state = 'IDLE' AND deadline IS NOT NULL AND deadline < NOW())
                       OR (state = 'CONNECTING' AND allocation_deadline IS NOT NULL AND allocation_deadline < NOW())
                       OR (state != 'CREATED' AND state != 'DEAD' AND last_activity_time < NOW())
                     LIMIT ?
                    """)) {
                s.setInt(1, limit);
                final var res = s.executeQuery();
                while (res.next()) {
                    final Vm vm = readVm(res);
                    vms.add(vm);
                }
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Cannot write vm", e);
            }
        });
        return vms;
    }

    @Nullable
    @Override
    public Vm acquire(String sessionId, String poolId, String zone, @Nullable TransactionHandle transaction) {
        final Vm[] vm = new Vm[1];
        final var tx = transaction == null ? new TransactionHandleImpl(storage) : transaction;
        DbOperation.execute(tx, storage, con -> {
            try (final var s = con.prepareStatement(
                "SELECT " + FIELDS + """
                    FROM vm
                    WHERE session_id = ? AND pool_label = ? AND zone = ? AND state = 'IDLE'
                    LIMIT 1
                    FOR UPDATE"""
            )) {
                s.setString(1, sessionId);
                s.setString(2, poolId);
                s.setString(3, zone);
                final var res = s.executeQuery();
                if (!res.next()) {
                    vm[0] = null;
                    return;
                }
                vm[0] = readVm(res);
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Cannot dump values", e);
            }
        });

        if (vm[0] != null) {
            vm[0] = new Vm.VmBuilder(vm[0]).setState(Vm.State.RUNNING).build();
            update(vm[0], tx);
        }

        if (transaction == null) {  // If executing in local transaction
            try {
                tx.commit();
                tx.close();
            } catch (SQLException e) {
                throw new RuntimeException("Cannot close transaction", e);
            }
        }

        return vm[0];
    }

    @Override
    public void saveAllocatorMeta(String vmId, Map<String, String> meta, @Nullable TransactionHandle transaction) {
        DbOperation.execute(transaction, storage, con -> {
            try (final var s = con.prepareStatement("""
                UPDATE vm SET allocator_meta_json = ?
                WHERE id = ?
                """)) {
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
    public Map<String, String> getAllocatorMeta(String vmId, @Nullable TransactionHandle transaction) {
        final AtomicReference<Map<String, String>> meta = new AtomicReference<>();
        DbOperation.execute(transaction, storage, con -> {
            try (final var s = con.prepareStatement("""
                SELECT allocator_meta_json FROM vm
                WHERE id = ?
                """)) {
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
        s.setString(7, objectMapper.writeValueAsString(vm.workloads()));
        s.setTimestamp(8, vm.lastActivityTime() == null ? null : Timestamp.from(vm.lastActivityTime()));
        s.setTimestamp(9, vm.deadline() == null ? null : Timestamp.from(vm.deadline()));
        s.setTimestamp(10, vm.allocationDeadline() == null ? null : Timestamp.from(vm.allocationDeadline()));
        s.setString(11, objectMapper.writeValueAsString(vm.vmMeta()));
    }

    private Vm readVm(ResultSet res) throws SQLException, JsonProcessingException {
        final var id = res.getString(1);
        final var sessionIdRes = res.getString(2);
        final var poolLabel = res.getString(3);
        final var zone = res.getString(4);
        final var state = Vm.State.valueOf(res.getString(5));
        final var allocationOpId = res.getString(6);
        final var workloads = objectMapper.readValue(res.getString(7),
            new TypeReference<List<Workload>>() {
            });

        final var lastActivityTimeTs = res.getTimestamp(8);
        final var lastActivityTime = lastActivityTimeTs == null ? null : lastActivityTimeTs.toInstant();

        final var deadlineTs = res.getTimestamp(9);
        final var deadline = deadlineTs == null ? null : deadlineTs.toInstant();

        final var allocationDeadlineTs = res.getTimestamp(10);
        final var allocationDeadline = allocationDeadlineTs == null ? null : allocationDeadlineTs.toInstant();

        final var vmMeta = objectMapper.readValue(res.getString(11),
            new TypeReference<Map<String, String>>() {
            });
        return new Vm(sessionIdRes, id, poolLabel, zone, state, allocationOpId, workloads,
            lastActivityTime, deadline, allocationDeadline, vmMeta);
    }
}
