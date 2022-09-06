package ai.lzy.allocator.disk;

import ai.lzy.v1.allocator.DiskApi;
import java.util.Objects;

public record Disk(
    String id,
    DiskSpec spec,
    DiskMeta meta
) {
    public DiskApi.Disk toProto() {
        return DiskApi.Disk.newBuilder()
            .setDiskId(id)
            .setSpec(DiskApi.DiskSpec.newBuilder()
                .setName(spec.name())
                .setType(DiskApi.DiskType.forNumber(spec.type().getValue()))
                .setSizeGb(spec.sizeGb())
                .setZoneId(spec.zone())
                .build())
            .build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Disk disk = (Disk) o;
        return id.equals(disk.id) && spec.equals(disk.spec) && meta.equals(disk.meta);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, spec, meta);
    }
}
