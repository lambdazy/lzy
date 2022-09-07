package ai.lzy.allocator.disk.impl.mock;

import ai.lzy.allocator.disk.Disk;
import ai.lzy.allocator.disk.DiskManager;
import ai.lzy.allocator.disk.DiskMeta;
import ai.lzy.allocator.disk.DiskSpec;
import ai.lzy.allocator.disk.exceptions.NotFoundException;
import io.micronaut.context.annotation.Requires;
import javax.annotation.Nullable;
import javax.inject.Singleton;
import org.apache.commons.lang.NotImplementedException;

@Requires(property = "allocator.yc-credentials.enabled", value = "false")
@Singleton
public class MockDiskManager implements DiskManager {
    @Nullable
    @Override
    public Disk get(String id) {
        throw new NotImplementedException("Not implemented");
    }

    @Override
    public Disk create(DiskSpec spec, DiskMeta meta) {
        throw new NotImplementedException("Not implemented");
    }

    @Override
    public Disk clone(Disk disk, DiskSpec cloneDiskSpec, DiskMeta meta) throws NotFoundException {
        throw new NotImplementedException("Not implemented");
    }

    @Override
    public void delete(String diskId) throws NotFoundException {
        throw new NotImplementedException("Not implemented");
    }
}
