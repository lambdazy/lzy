package ai.lzy.allocator.volume.dao;

import ai.lzy.allocator.model.VolumeClaim;
import ai.lzy.model.db.TransactionHandle;
import io.micronaut.core.annotation.Nullable;

import java.sql.SQLException;

public interface VolumeClaimDao {
    void create(VolumeClaim volumeClaim, @Nullable TransactionHandle tx) throws SQLException;

    void delete(String id, @Nullable TransactionHandle tx) throws SQLException;

    @Nullable
    VolumeClaim get(String id, @Nullable TransactionHandle tx) throws SQLException;

    long countForVolumeId(String volumeId, @Nullable TransactionHandle tx) throws SQLException;
}
