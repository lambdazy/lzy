package ai.lzy.allocator.model;

import ai.lzy.v1.VolumeApi;
import com.fasterxml.jackson.annotation.*;

import java.util.Objects;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class VolumeRequest {
    private final String volumeId;
    private final VolumeDescription volumeDescription;

    @JsonCreator
    public VolumeRequest(@JsonProperty("volumeId") String volumeId,
                         @JsonProperty("volumeDescription") VolumeDescription volumeDescription)
    {
        this.volumeId = volumeId;
        this.volumeDescription = volumeDescription;
    }

    public String volumeId() {
        return volumeId;
    }

    public VolumeDescription volumeDescription() {
        return volumeDescription;
    }

    @Override
    public String toString() {
        return "VolumeRequest{" +
            "volumeId=" + volumeId +
            ", volumeDescription=" + volumeDescription +
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
        VolumeRequest that = (VolumeRequest) o;
        return Objects.equals(volumeId, that.volumeId)
            && Objects.equals(volumeDescription, that.volumeDescription);
    }

    @Override
    public int hashCode() {
        return Objects.hash(volumeId, volumeDescription);
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "type")
    @JsonSubTypes({
        @JsonSubTypes.Type(value = DiskVolumeDescription.class),
        @JsonSubTypes.Type(value = HostPathVolumeDescription.class),
        @JsonSubTypes.Type(value = NFSVolumeDescription.class)
    })
    public abstract static class VolumeDescription {
        public abstract String name();
    }

    public abstract static class ResourceVolumeDescription extends VolumeDescription {
    }

    public VolumeApi.VolumeRequest toProto() {
        var builder = VolumeApi.VolumeRequest.newBuilder()
            .setId(volumeId)
            .setName(volumeDescription.name());
        if (volumeDescription instanceof DiskVolumeDescription diskVolumeDescription) {
            var diskVolumeBuilder = VolumeApi.DiskVolumeType.newBuilder()
                .setDiskId(diskVolumeDescription.diskId())
                .setSizeGb(diskVolumeDescription.sizeGb());
            var accessMode = diskVolumeDescription.accessMode();
            if (accessMode != null) {
                diskVolumeBuilder.setAccessMode(accessMode.toProto());
            }
            var storageClass = diskVolumeDescription.storageClass();
            if (storageClass != null) {
                diskVolumeBuilder.setStorageClass(storageClass.toProto());
            }
            var fsType = diskVolumeDescription.fsType();
            if (fsType != null) {
                diskVolumeBuilder.setFsType(fsType.toProto());
            }
            builder.setDiskVolume(diskVolumeBuilder.build());
        } else if (volumeDescription instanceof HostPathVolumeDescription hostPathVolumeDescription) {
            builder.setHostPathVolume(VolumeApi.HostPathVolumeType.newBuilder()
                .setPath(hostPathVolumeDescription.path())
                .setHostPathType(hostPathVolumeDescription.hostPathType().toProto())
                .build());
        } else if (volumeDescription instanceof NFSVolumeDescription nfsVolumeDescription) {
            builder.setNfsVolume(VolumeApi.NFSVolumeType.newBuilder()
                .setServer(nfsVolumeDescription.server())
                .setShare(nfsVolumeDescription.share())
                .setReadOnly(nfsVolumeDescription.readOnly())
                .addAllMountOptions(nfsVolumeDescription.mountOptions())
                .build());
        }
        return builder.build();
    }
}
