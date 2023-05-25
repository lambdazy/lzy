package ai.lzy.allocator.model;

import ai.lzy.v1.VolumeApi;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
@JsonSerialize
@JsonDeserialize
public record Volume(
    String name,
    String volumeRequestName,
    String diskId,
    int sizeGb,
    AccessMode accessMode,
    String storageClass
) {
    public enum AccessMode {
        READ_WRITE_ONCE("ReadWriteOnce"),
        READ_ONLY_MANY("ReadOnlyMany"),
        READ_WRITE_MANY("ReadWriteMany"),
        READ_WRITE_ONCE_POD("ReadWriteOncePod");

        private final String asString;

        AccessMode(String asString) {
            this.asString = asString;
        }

        public String asString() {
            return asString;
        }

        public static AccessMode fromString(String s) {
            return switch (s) {
                case "ReadWriteOnce" -> READ_WRITE_ONCE;
                case "ReadOnlyMany" -> READ_ONLY_MANY;
                case "ReadWriteMany" -> READ_WRITE_MANY;
                case "ReadWriteOncePod" -> READ_WRITE_ONCE_POD;
                default -> throw new IllegalArgumentException("Unknown volume access mode " + s);
            };
        }

        public VolumeApi.DiskVolumeType.AccessMode toProto() {
            return switch (this) {
                case READ_WRITE_ONCE -> VolumeApi.DiskVolumeType.AccessMode.READ_WRITE_ONCE;
                case READ_ONLY_MANY -> VolumeApi.DiskVolumeType.AccessMode.READ_ONLY_MANY;
                case READ_WRITE_MANY -> VolumeApi.DiskVolumeType.AccessMode.READ_WRITE_MANY;
                case READ_WRITE_ONCE_POD -> VolumeApi.DiskVolumeType.AccessMode.READ_WRITE_ONCE_POD;
            };
        }
    }
}
