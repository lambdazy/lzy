package ai.lzy.allocator.disk;

import ai.lzy.v1.DiskApi;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonSerialize
@JsonDeserialize
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
            .setOwner(meta.user())
            .build();
    }

    public static Disk fromProto(DiskApi.Disk disk) {
        return new Disk(disk.getDiskId(), DiskSpec.fromProto(disk.getSpec()), new DiskMeta(disk.getOwner()));
    }
}
