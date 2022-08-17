package ai.lzy.allocator.disk.util;

import ai.lzy.allocator.disk.*;
import ai.lzy.v1.allocator.DiskApi;

public class GrpcConverter {
    public static Disk from(DiskApi.Disk disk) {
        return new Disk(disk.getDiskId(), from(disk.getSpec()));
    }

    public static DiskSpec from(DiskApi.DiskSpec diskSpec) {
        return new DiskSpec(
            diskSpec.getName(),
            DiskType.fromNumber(diskSpec.getType().getNumber()),
            diskSpec.getSizeGb(),
            diskSpec.getZoneId()
        );
    }
}
