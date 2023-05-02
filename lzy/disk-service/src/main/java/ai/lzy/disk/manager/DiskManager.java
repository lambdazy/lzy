package ai.lzy.disk.manager;

import ai.lzy.disk.model.Disk;
import ai.lzy.disk.model.DiskType;

import jakarta.annotation.Nullable;
import javax.annotation.Nullable;

public interface DiskManager {

    Disk createDisk(String userId, String label, DiskType diskType, int diskSizeGb);

    @Nullable
    Disk findDisk(String userId, String diskId);

    void deleteDisk(String userId, String diskId);

    default boolean isDiskExists(String userId, String diskId) {
        return findDisk(userId, diskId) != null;
    }

}
