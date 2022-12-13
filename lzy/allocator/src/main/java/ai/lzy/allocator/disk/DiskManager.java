package ai.lzy.allocator.disk;

import io.grpc.StatusException;

import java.time.Instant;
import javax.annotation.Nullable;

public interface DiskManager {

    record OuterOperation(
        String opId,
        Instant startedAt,
        Instant deadline
    ) {}

    @Nullable
    Disk get(String id) throws StatusException;

    DiskOperation newCreateDiskOperation(OuterOperation outerOp, DiskSpec spec, DiskMeta meta);

    DiskOperation newCloneDiskOperation(OuterOperation outerOp, Disk disk, DiskSpec newDiskSpec, DiskMeta newDiskMeta);

    DiskOperation newDeleteDiskOperation(OuterOperation outerOp, String diskId);

    DiskOperation restoreDiskOperation(DiskOperation template);
}