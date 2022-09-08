package ai.lzy.allocator.disk.impl.mock;

import ai.lzy.allocator.disk.Disk;
import ai.lzy.allocator.disk.DiskManager;
import ai.lzy.allocator.disk.DiskMeta;
import ai.lzy.allocator.disk.DiskSpec;
import ai.lzy.allocator.disk.exceptions.NotFoundException;
import io.micronaut.context.annotation.Requires;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import javax.inject.Singleton;

@Requires(property = "allocator.yc-credentials.enabled", value = "false")
@Singleton
public class MockDiskManager implements DiskManager {
    private final Map<String, Disk> disks = new ConcurrentHashMap<>();

    @Nullable
    @Override
    public Disk get(String id) {
        return disks.get(id);
    }

    @Override
    public Disk create(DiskSpec spec, DiskMeta meta) {
        final String id = UUID.randomUUID().toString();
        final Disk disk = new Disk(id, spec, meta);
        disks.put(id, disk);
        return disk;
    }

    @Override
    public Disk clone(Disk disk, DiskSpec cloneDiskSpec, DiskMeta meta) throws NotFoundException {
        if (!disks.containsKey(disk.id())) {
            throw new NotFoundException();
        }

        if (disk.spec().sizeGb() > cloneDiskSpec.sizeGb()) {
            throw new IllegalArgumentException("Cannot decrease size during clone");
        }

        final String id = UUID.randomUUID().toString();
        final Disk clonedDisk = new Disk(id, cloneDiskSpec, meta);
        disks.put(id, clonedDisk);
        return clonedDisk;
    }

    @Override
    public void delete(String diskId) throws NotFoundException {
        if (!disks.containsKey(diskId)) {
            throw new NotFoundException();
        }

        disks.remove(diskId);
    }
}
