package ai.lzy.allocator.disk;

import ai.lzy.allocator.disk.exceptions.DiskNotFoundException;
import javax.annotation.Nullable;

public interface DiskManager {
    @Nullable
    Disk get(String id);

    Disk create(DiskSpec spec);

    Disk clone(Disk disk, DiskSpec cloneDiskSpec) throws DiskNotFoundException;

    void delete(Disk disk) throws DiskNotFoundException;
}
