package ai.lzy.allocator.disk;

import ai.lzy.allocator.disk.exceptions.InternalErrorException;
import ai.lzy.allocator.disk.exceptions.NotFoundException;
import javax.annotation.Nullable;

public interface DiskManager {
    @Nullable
    Disk get(String id);

    Disk create(DiskSpec spec) throws InternalErrorException, InterruptedException;

    Disk clone(Disk disk, DiskSpec cloneDiskSpec)
        throws NotFoundException, InternalErrorException, InterruptedException;

    void delete(Disk disk)
        throws NotFoundException, InternalErrorException, InterruptedException;
}
