package ai.lzy.allocator.disk;

import ai.lzy.v1.DiskApi;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonSerialize
@JsonDeserialize
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

    public DiskApi.DiskSpec toProto() {
        return DiskApi.DiskSpec.newBuilder()
            .setName(name())
            .setType(DiskApi.DiskType.forNumber(type().getValue()))
            .setSizeGb(sizeGb())
            .setZoneId(zone())
            .build();
    }
}
