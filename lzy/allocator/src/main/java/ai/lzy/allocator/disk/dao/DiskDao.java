package ai.lzy.allocator.disk.dao;

import ai.lzy.allocator.disk.Disk;
import ai.lzy.allocator.disk.DiskMeta;
import ai.lzy.allocator.disk.DiskSpec;
import ai.lzy.allocator.disk.DiskType;
import ai.lzy.allocator.storage.AllocatorDataSource;
import ai.lzy.model.db.DbOperation;
import ai.lzy.model.db.Storage;
import ai.lzy.model.db.TransactionHandle;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;
import javax.annotation.Nullable;

@Singleton
public class DiskDao {
    private static final Logger LOG = LogManager.getLogger(DiskDao.class);

    private static final String FIELDS =
        "id, name, type, size_gb, zone_id, user_id";

    private static final String QUERY_INSERT_DISK = """
        INSERT INTO disk (%s)
        VALUES (?, ?, ?, ?, ?, ?)""".formatted(FIELDS);

    private static final String QUERY_GET_DISK = """
        SELECT %s
        FROM disk
        WHERE id = ?""".formatted(FIELDS);

    private static final String QUERY_REMOVE_DISK = """
        DELETE
        FROM disk
        WHERE id = ?""";

    private final Storage storage;

    @Inject
    public DiskDao(AllocatorDataSource storage) {
        this.storage = storage;
    }

    public void insert(Disk disk, @Nullable TransactionHandle transaction) throws SQLException {
        LOG.info("Insert into storage disk=" + disk);
        DbOperation.execute(transaction, storage, con -> {
            try (final var s = con.prepareStatement(QUERY_INSERT_DISK)) {
                s.setString(1, disk.id());
                final DiskSpec spec = disk.spec();
                s.setString(2, spec.name());
                s.setString(3, spec.type().name());
                s.setInt(4, spec.sizeGb());
                s.setString(5, spec.zone());
                s.setString(6, disk.meta().user());
                s.execute();
            }
        });
    }

    @Nullable
    public Disk get(String diskId, @Nullable TransactionHandle transaction) throws SQLException {
        LOG.info("Get from storage diskId=" + diskId);
        final Disk[] disk = {null};
        DbOperation.execute(transaction, storage, con -> {
            try (final var s = con.prepareStatement(QUERY_GET_DISK)) {
                s.setString(1, diskId);
                final var res = s.executeQuery();
                if (!res.next()) {
                    disk[0] = null;
                    return;
                }
                final String id = res.getString(1);
                assert id.equals(diskId);
                final String name = res.getString(2);
                final String type = res.getString(3);
                final int sizeGb = res.getInt(4);
                final String zone = res.getString(5);
                final String userId = res.getString(6);
                disk[0] = new Disk(diskId,
                    new DiskSpec(name, DiskType.valueOf(type), sizeGb, zone), new DiskMeta(userId));
            }
        });
        return disk[0];
    }

    public void remove(String diskId, @Nullable TransactionHandle transaction) throws SQLException {
        LOG.info("Remove from storage diskId=" + diskId);
        DbOperation.execute(transaction, storage, con -> {
            try (final var s = con.prepareStatement(QUERY_REMOVE_DISK)) {
                s.setString(1, diskId);
                s.execute();
            }
        });
    }
}
