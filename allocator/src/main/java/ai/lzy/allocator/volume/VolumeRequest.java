package ai.lzy.allocator.volume;

import ai.lzy.v1.VolumeApi;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.util.Objects;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class VolumeRequest {
    private final VolumeDescription volumeDescription;

    @JsonCreator
    public VolumeRequest(@JsonProperty("volumeDescription") VolumeDescription volumeDescription) {
        this.volumeDescription = volumeDescription;
    }

    public String name() {
        return volumeDescription.name();
    }

    public VolumeDescription volumeDescription() {
        return volumeDescription;
    }

    @Override
    public String toString() {
        return "VolumeRequest{" +
            "volumeDescription=" + volumeDescription +
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
        return Objects.equals(volumeDescription, that.volumeDescription);
    }

    @Override
    public int hashCode() {
        return Objects.hash(volumeDescription);
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "type")
    @JsonSubTypes({
        @JsonSubTypes.Type(value = DiskVolumeDescription.class),
        @JsonSubTypes.Type(value = HostPathVolumeDescription.class)
    })
    public abstract static class VolumeDescription {
        public abstract String name();
    }

    public static VolumeRequest fromProto(VolumeApi.Volume volume) {
        return new VolumeRequest(
            switch (volume.getVolumeTypeCase()) {
                case DISK_VOLUME -> new DiskVolumeDescription(volume.getName(), volume.getDiskVolume().getDiskId());
                case HOST_PATH_VOLUME -> new HostPathVolumeDescription(
                    volume.getName(),
                    volume.getHostPathVolume().getPath(),
                    HostPathVolumeDescription.HostPathType.valueOf(volume.getHostPathVolume().getHostPathType().name())
                );
                default -> throw new IllegalArgumentException("unknown volume type " + volume.getVolumeTypeCase());
            }
        );
    }
}
