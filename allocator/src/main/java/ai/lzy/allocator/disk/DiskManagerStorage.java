package ai.lzy.allocator.disk;

import ai.lzy.allocator.dao.impl.AllocatorDataSource;
import jakarta.inject.Inject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;

public class DiskManagerStorage {
    private static final Logger LOG = LogManager.getLogger(DiskManagerStorage.class);

    private final AllocatorDataSource storage;

    @Inject
    private DiskManagerStorage(AllocatorDataSource storage) {
        this.storage = storage;
    }

    void insert(Connection sqlConnection) {}

    Disk get(Connection sqlConnection) {
        return null;
    }

    void remove(Connection sqlConnection) {}

}
