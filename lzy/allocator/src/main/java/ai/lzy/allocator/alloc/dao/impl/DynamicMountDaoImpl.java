package ai.lzy.allocator.alloc.dao.impl;

import ai.lzy.allocator.alloc.dao.DynamicMountDao;
import ai.lzy.allocator.model.DynamicMount;
import ai.lzy.allocator.model.VolumeRequest;
import ai.lzy.allocator.storage.AllocatorDataSource;
import ai.lzy.model.db.DbOperation;
import ai.lzy.model.db.TransactionHandle;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.jetbrains.annotations.Nullable;
import org.postgresql.util.PGobject;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@Singleton
public class DynamicMountDaoImpl implements DynamicMountDao {
    private static final String ALL_FIELDS = "id, vm_id, cluster_id, volume_desc, mount_path, mount_name, worker_id," +
        " mount_op_id, volume_name, volume_claim_name, mount_op_id, unmount_op_id, state";

    private static final String CREATE_DYNAMIC_MOUNT_QUERY = """
        INSERT INTO dynamic_mount (id, vm_id, cluster_id, volume_desc, mount_path, mount_name,
            worker_id, mount_op_id, state)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """;

    private static final String DELETE_DYNAMIC_MOUNT_QUERY = """
        DELETE FROM dynamic_mount WHERE id = ?
        """;

    private static final String COUNT_BY_VOLUME_CLAIM_NAME_QUERY = """
        SELECT COUNT(*) FROM dynamic_mount WHERE cluster_id = ? AND volume_claim_name = ?
        """;

    private static final String GET_DYNAMIC_MOUNT_QUERY = """
        SELECT %s FROM dynamic_mount WHERE id = ?
        """.formatted(ALL_FIELDS);

    private static final String GET_PENDING_DYNAMIC_MOUNTS_QUERY = """
        SELECT %s FROM dynamic_mount WHERE state = 'PENDING' and worker_id = ?
        """.formatted(ALL_FIELDS);

    private static final String GET_DELETING_DYNAMIC_MOUNTS_QUERY = """
        SELECT %s FROM dynamic_mount WHERE state = 'DELETING' and worker_id = ?
        """.formatted(ALL_FIELDS);

    private final AllocatorDataSource storage;
    private final ObjectMapper objectMapper;

    public DynamicMountDaoImpl(AllocatorDataSource storage, @Named("AllocatorObjectMapper") ObjectMapper objectMapper) {
        this.storage = storage;
        this.objectMapper = objectMapper;
    }

    @Override
    public void create(DynamicMount dynamicMount, @Nullable TransactionHandle tx) throws SQLException {
        DbOperation.execute(tx, storage, con -> {
            try (PreparedStatement s = con.prepareStatement(CREATE_DYNAMIC_MOUNT_QUERY)) {
                int idx = 0;
                s.setString(++idx, dynamicMount.id());
                s.setString(++idx, dynamicMount.vmId());
                s.setString(++idx, dynamicMount.clusterId());
                var jsonObject = new PGobject();
                jsonObject.setType("json");
                jsonObject.setValue(objectMapper.writeValueAsString(dynamicMount.volumeDescription()));
                s.setObject(++idx, jsonObject);
                s.setString(++idx, dynamicMount.mountPath());
                s.setString(++idx, dynamicMount.mountName());
                s.setString(++idx, dynamicMount.workerId());
                s.setString(++idx, dynamicMount.mountOperationId());
                s.setString(++idx, dynamicMount.state().name());
                s.execute();
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Cannot write volume description: %s"
                    .formatted(dynamicMount.volumeDescription()), e);
            }
        });
    }

    @Override
    @Nullable
    public DynamicMount get(String id, @Nullable TransactionHandle tx) throws SQLException {
        return DbOperation.execute(tx, storage, con -> {
           try (PreparedStatement s = con.prepareStatement(GET_DYNAMIC_MOUNT_QUERY)) {
               s.setString(1, id);
               final var rs = s.executeQuery();
               if (rs.next()) {
                   return readDynamicMount(rs);
               }
               return null;
           }
        });
    }

    @Override
    public void delete(String id, @Nullable TransactionHandle tx) throws SQLException {
        DbOperation.execute(tx, storage, con -> {
            try (PreparedStatement s = con.prepareStatement(DELETE_DYNAMIC_MOUNT_QUERY)) {
                s.setString(1, id);
                s.execute();
            }
        });
    }

    @Override
    @Nullable
    public DynamicMount update(String id, DynamicMount.Update update, @Nullable TransactionHandle tx) throws SQLException {
        if (update.isEmpty()) {
            throw new IllegalArgumentException("Update is empty");
        }
        return DbOperation.execute(tx, storage, con -> {
            try (PreparedStatement s = con.prepareStatement(prepareUpdateStatement(update))) {
                prepareUpdateParameters(id, update, s);
                var rs = s.executeQuery();
                if (rs.next()) {
                    return readDynamicMount(rs);
                }
                return null;
            }
        });
    }

    @Override
    public long countForVolumeClaimName(String clusterId, String volumeClaimName, @Nullable TransactionHandle tx)
        throws SQLException
    {
        return DbOperation.execute(tx, storage, con -> {
            try (PreparedStatement s = con.prepareStatement(COUNT_BY_VOLUME_CLAIM_NAME_QUERY)) {
                s.setString(1, clusterId);
                s.setString(2, volumeClaimName);
                final var rs = s.executeQuery();
                if (rs.next()) {
                    return rs.getLong(1);
                }
                return 0L;
            }
        });
    }

    @Override
    public List<DynamicMount> getPending(String workerId, @Nullable TransactionHandle tx) throws SQLException {
        return DbOperation.execute(tx, storage, con -> {
            try (PreparedStatement s = con.prepareStatement(GET_PENDING_DYNAMIC_MOUNTS_QUERY)) {
                s.setString(1, workerId);
                final var rs = s.executeQuery();
                var result = new ArrayList<DynamicMount>();
                while (rs.next()) {
                    result.add(readDynamicMount(rs));
                }
                return result;
            }
        });
    }

    @Override
    public List<DynamicMount> getDeleting(String workerId, @Nullable TransactionHandle tx) throws SQLException {
        return DbOperation.execute(tx, storage, con -> {
            try (PreparedStatement s = con.prepareStatement(GET_DELETING_DYNAMIC_MOUNTS_QUERY)) {
                s.setString(1, workerId);
                final var rs = s.executeQuery();
                var result = new ArrayList<DynamicMount>();
                while (rs.next()) {
                    result.add(readDynamicMount(rs));
                }
                return result;
            }
        });
    }

    private static String prepareUpdateStatement(DynamicMount.Update update) {
        var sb = new StringBuilder("UPDATE dynamic_mount SET ");
        if (update.volumeName() != null) {
            sb.append("volume_name = ?, ");
        }
        if (update.volumeClaimName() != null) {
            sb.append("volume_claim_name = ?, ");
        }
        if (update.unmountOperationId() != null) {
            sb.append("unmount_op_id = ?, ");
        }
        if (update.state() != null) {
            sb.append("state = ?, ");
        }
        sb.setLength(sb.length() - 2);
        sb.append(" WHERE id = ? RETURNING ").append(ALL_FIELDS);
        return sb.toString();
    }

    private static void prepareUpdateParameters(String id, DynamicMount.Update update, PreparedStatement s)
        throws SQLException
    {
        int idx = 0;
        if (update.volumeName() != null) {
            s.setString(++idx, update.volumeName());
        }
        if (update.volumeClaimName() != null) {
            s.setString(++idx, update.volumeClaimName());
        }
        if (update.unmountOperationId() != null) {
            s.setString(++idx, update.unmountOperationId());
        }
        if (update.state() != null) {
            s.setString(++idx, update.state().name());
        }
        s.setString(++idx, id);
    }

    private DynamicMount readDynamicMount(ResultSet rs) {
        try {
            var id = rs.getString("id");
            var vmId = rs.getString("vm_id");
            var clusterId = rs.getString("cluster_id");
            var volumeDesc = objectMapper.readValue(rs.getString("volume_desc"),
                VolumeRequest.ResourceVolumeDescription.class);
            var mountPath = rs.getString("mount_path");
            var mountName = rs.getString("mount_name");
            var workerId = rs.getString("worker_id");
            var mountOpId = rs.getString("mount_op_id");
            var unmountOpId = rs.getString("unmount_op_id");
            var volumeName = rs.getString("volume_name");
            var volumeClaimName = rs.getString("volume_claim_name");
            var state = DynamicMount.State.valueOf(rs.getString("state"));
            return new DynamicMount(id, vmId, clusterId, mountPath, mountName, volumeName, volumeClaimName, volumeDesc,
                mountOpId, unmountOpId, state, workerId);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Cannot read volume description", e);
        } catch (SQLException e) {
            throw new RuntimeException("Cannot read dynamic mount", e);
        }
    }
}
