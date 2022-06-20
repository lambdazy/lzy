package ai.lzy.disk.manager;

import ai.lzy.disk.Disk;
import ai.lzy.disk.DiskType;
import javax.annotation.Nullable;

public interface DiskManager {

    Disk createDisk(String label, DiskType diskType);

    @Nullable
    Disk findDisk(String diskId);

    void deleteDisk(String diskId);

    default boolean isDiskExists(String diskId) {
        return findDisk(diskId) != null;
    }

}
