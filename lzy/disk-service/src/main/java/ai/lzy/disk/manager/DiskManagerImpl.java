package ai.lzy.disk.manager;

import ai.lzy.disk.dao.DiskDao;
import ai.lzy.disk.model.Disk;
import ai.lzy.disk.model.DiskSpec;
import ai.lzy.disk.model.DiskType;
import ai.lzy.disk.providers.DiskStorageProviderResolver;
import jakarta.inject.Inject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.UUID;
import jakarta.annotation.Nullable;
import javax.annotation.Nullable;

public class DiskManagerImpl implements DiskManager {

    private static final Logger LOG = LogManager.getLogger(DiskManagerImpl.class);

    @Inject
    private DiskDao diskDao;
    @Inject
    private DiskStorageProviderResolver diskResolver;

    @Override
    public Disk createDisk(String userId, String label, DiskType diskType, int diskSizeGb) {
        final String diskId = UUID.randomUUID().toString();
        final DiskSpec diskSpec = diskResolver.getProvider(diskType).createDisk(label, diskId, diskSizeGb);
        final Disk disk = new Disk(diskId, diskSpec);
        diskDao.insert(userId, disk);
        return disk;
    }

    @Nullable
    @Override
    public Disk findDisk(String userId, String diskId) {
        Disk disk = diskDao.find(userId, diskId);
        if (disk == null) {
            return null;
        }
        if (!diskResolver.getProvider(disk.type()).isExistDisk(disk.spec())) {
            LOG.warn("Disk {} doesn't exist, but disk info still in database", diskId);
            return null;
        }
        return disk;
    }

    @Override
    public void deleteDisk(String userId, String diskId) {
        Disk disk = findDisk(userId, diskId);
        if (disk != null) {
            diskDao.delete(userId, disk.id());
            diskResolver.getProvider(disk.type()).deleteDisk(disk.spec());
        }
    }

}
