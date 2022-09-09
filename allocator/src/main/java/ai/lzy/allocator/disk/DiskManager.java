package ai.lzy.allocator.disk;

import ai.lzy.allocator.disk.exceptions.NotFoundException;
import javax.annotation.Nullable;

public interface DiskManager {
    @Nullable
    Disk get(String id);

    Disk create(DiskSpec spec, DiskMeta meta);

    Disk clone(Disk disk, DiskSpec cloneDiskSpec, DiskMeta clonedDiskMeta) throws NotFoundException;

    void delete(String diskId) throws NotFoundException;
}
