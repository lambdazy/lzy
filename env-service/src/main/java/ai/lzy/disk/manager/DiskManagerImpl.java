package ai.lzy.disk.manager;

import ai.lzy.disk.Disk;
import ai.lzy.disk.DiskSpec;
import ai.lzy.disk.DiskType;
import ai.lzy.disk.providers.DiskProviderResolver;
import ai.lzy.disk.dao.DiskDao;
import jakarta.inject.Inject;
import java.util.UUID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DiskManagerImpl implements DiskManager {

    private static final Logger LOG = LogManager.getLogger(DiskManagerImpl.class);

    @Inject
    private DiskDao diskDao;
    @Inject
    private DiskProviderResolver diskResolver;


    @Override
    public Disk createDisk(String label, DiskType diskType) {
        final String diskId = UUID.randomUUID().toString();
        final DiskSpec diskSpec = diskResolver.getProvider(diskType).createDisk(label, diskId);
        final Disk disk = new Disk(diskId, diskSpec);
        diskDao.insert(disk);
        return disk;
    }

    @Override
    public Disk findDisk(String diskId) {
        Disk disk = diskDao.find(diskId);
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
    public void deleteDisk(String diskId) {
        Disk disk = findDisk(diskId);
        if (disk != null) {
            diskDao.delete(disk.id());
            diskResolver.getProvider(disk.type()).deleteDisk(disk.spec());
        }
    }
}
