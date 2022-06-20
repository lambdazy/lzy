package ai.lzy.disk.providers;

import ai.lzy.disk.DiskSpec;
import ai.lzy.disk.DiskType;

public interface DiskProvider {

    DiskType getType();

    DiskSpec createDisk(String label, String diskId);

    boolean isExistDisk(DiskSpec diskSpec);

    void deleteDisk(DiskSpec diskSpec);

}
