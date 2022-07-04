package ai.lzy.disk.providers;

import ai.lzy.disk.DiskSpec;
import ai.lzy.disk.DiskType;

public interface DiskStorageProvider {

    DiskType getType();

    DiskSpec createDisk(String label, String diskId, int diskSizeGb);

    boolean isExistDisk(DiskSpec diskSpec);

    void deleteDisk(DiskSpec diskSpec);

}
