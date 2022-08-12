package ai.lzy.allocator.disk;

import ai.lzy.model.db.Storage;
import jakarta.inject.Inject;
import java.sql.Connection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DiskManagerStorage {
    private static final Logger LOG = LogManager.getLogger(DiskManagerStorage.class);

    private final Storage storage;

    @Inject
    private DiskManagerStorage(DiskManagerDataSource storage) {
        this.storage = storage;
    }

    void insert(Connection sqlConnection) {}

    Disk get(Connection sqlConnection) {
        return null;
    }

    void remove(Connection sqlConnection) {}

}
