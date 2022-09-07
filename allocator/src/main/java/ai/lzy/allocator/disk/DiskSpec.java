package ai.lzy.allocator.disk;

import ai.lzy.v1.allocator.DiskApi;

public record DiskSpec(
    String name,
    DiskType type,
    int sizeGb,
    String zone
) {
    public static DiskSpec fromProto(DiskApi.DiskSpec diskSpec) {
        return new DiskSpec(
            diskSpec.getName(),
            DiskType.valueOf(diskSpec.getType().name()),
            diskSpec.getSizeGb(),
            diskSpec.getZoneId()
        );
    }
}
