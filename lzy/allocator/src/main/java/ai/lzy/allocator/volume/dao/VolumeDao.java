package ai.lzy.allocator.volume.dao;

import ai.lzy.allocator.model.Volume;
import ai.lzy.model.db.TransactionHandle;
import io.micronaut.core.annotation.Nullable;

import java.sql.SQLException;

public interface VolumeDao {
    void create(Volume volume, @Nullable TransactionHandle tx) throws SQLException;

    void delete(String id, @Nullable TransactionHandle tx) throws SQLException;

    @Nullable
    Volume get(String id, @Nullable TransactionHandle tx) throws SQLException;

}
