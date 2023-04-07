package ai.lzy.allocator.alloc.dao;

import ai.lzy.allocator.model.DynamicMount;
import ai.lzy.model.db.TransactionHandle;
import org.jetbrains.annotations.Nullable;

import java.sql.SQLException;
import java.util.List;

public interface DynamicMountDao {
    void create(DynamicMount dynamicMount, @Nullable TransactionHandle tx) throws SQLException;

    @Nullable
    DynamicMount get(String id, @Nullable TransactionHandle tx) throws SQLException;

    void delete(String id, @Nullable TransactionHandle tx) throws SQLException;

    @Nullable
    DynamicMount update(String id, DynamicMount.Update update, @Nullable TransactionHandle tx) throws SQLException;

    long countForVolumeClaimId(String volumeClaimId, @Nullable TransactionHandle tx) throws SQLException;

    List<DynamicMount> getPending(String workerId, @Nullable TransactionHandle tx) throws SQLException;

    List<DynamicMount> getDeleting(String workerId, @Nullable TransactionHandle tx) throws SQLException;
}
