package ai.lzy.allocator.alloc.dao;

import ai.lzy.allocator.model.DynamicMount;
import ai.lzy.model.db.TransactionHandle;
import jakarta.annotation.Nullable;

import java.sql.SQLException;
import java.util.List;

public interface DynamicMountDao {
    DynamicMount create(DynamicMount dynamicMount, @Nullable TransactionHandle tx) throws SQLException;

    @Nullable
    DynamicMount get(String id, @Nullable TransactionHandle tx) throws SQLException;

    void setMountName(String id, String mountName, @Nullable TransactionHandle tx) throws SQLException;

    void delete(String id, @Nullable TransactionHandle tx) throws SQLException;

    void setDeleting(String id, String unmountOpId, @Nullable TransactionHandle tx) throws SQLException;

    void setUnmountOperationId(String id, String unmountOperationId, @Nullable TransactionHandle tx)
        throws SQLException;

    long countForVolumeClaimId(String volumeClaimId, @Nullable TransactionHandle tx) throws SQLException;

    List<DynamicMount> getNonDeletingByVmId(String vmId, @Nullable TransactionHandle tx) throws SQLException;
}
