package ai.lzy.allocator.model;

import ai.lzy.v1.VolumeApi;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.annotation.Nullable;

import java.util.Objects;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class DiskVolumeDescription extends VolumeRequest.ResourceVolumeDescription {
    private final String name;
    private final String diskId;
    private final int sizeGb;
    @Nullable
    private final Volume.AccessMode accessMode;
    @Nullable
    private final StorageClass storageClass;

    @JsonCreator
    public DiskVolumeDescription(@JsonProperty("name") String name,
                                 @JsonProperty("diskId") String diskId,
                                 @JsonProperty("sizeGb") int sizeGb,
                                 @JsonProperty("accessMode") @Nullable Volume.AccessMode accessMode,
                                 @JsonProperty("storageClass") @Nullable StorageClass storageClass)
    {
        this.name = name;
        this.diskId = diskId;
        this.sizeGb = sizeGb;
        this.accessMode = accessMode;
        this.storageClass = storageClass;
    }

    @Override
    public String name() {
        return name;
    }

    public String diskId() {
        return diskId;
    }

    public int sizeGb() {
        return sizeGb;
    }

    @Nullable
    public Volume.AccessMode accessMode() {
        return accessMode;
    }

    @Nullable
    public StorageClass storageClass() {
        return storageClass;
    }

    @Override
    public String toString() {
        return "DiskVolumeDescription{" +
            "name='" + name + '\'' +
            ", diskId='" + diskId + '\'' +
            ", sizeGb=" + sizeGb +
            ", accessMode=" + accessMode +
            ", storageClass=" + storageClass +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DiskVolumeDescription that = (DiskVolumeDescription) o;
        return name.equals(that.name) && diskId.equals(that.diskId) && Objects.equals(accessMode, that.accessMode)
            && Objects.equals(storageClass, that.storageClass);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, diskId, accessMode, storageClass);
    }

    public enum StorageClass {
        HDD,
        SSD,
        ;

        public VolumeApi.DiskVolumeType.StorageClass toProto() {
            return switch (this) {
                case HDD -> VolumeApi.DiskVolumeType.StorageClass.HDD;
                case SSD -> VolumeApi.DiskVolumeType.StorageClass.SSD;
            };
        }
    }
}
