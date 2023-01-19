package ai.lzy.allocator.disk;

import ai.lzy.allocator.exceptions.InvalidConfigurationException;
import ai.lzy.longrunning.Operation;
import io.grpc.StatusException;

import java.time.Instant;
import javax.annotation.Nullable;

public interface DiskManager {

    record OuterOperation(
        String opId,
        String descr,
        Instant startedAt,
        Instant deadline
    ) {
        public static OuterOperation from(Operation op) {
            return new OuterOperation(op.id(), op.description(), op.createdAt(), op.deadline());
        }
    }

    @Nullable
    Disk get(String id) throws StatusException;

    DiskOperation newCreateDiskOperation(OuterOperation outerOp, DiskSpec spec, DiskMeta meta)
        throws InvalidConfigurationException;

    DiskOperation newCloneDiskOperation(OuterOperation outerOp, Disk disk, DiskSpec newDiskSpec, DiskMeta newDiskMeta);

    DiskOperation newDeleteDiskOperation(OuterOperation outerOp, String diskId);

    DiskOperation restoreDiskOperation(DiskOperation template);
}
