package ai.lzy.allocator.volume;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class DiskVolumeDescription extends VolumeRequest.VolumeDescription {
    private final String name;
    private final String diskId;

    @JsonCreator
    public DiskVolumeDescription(@JsonProperty("name") String name, @JsonProperty("diskId") String diskId) {
        this.name = name;
        this.diskId = diskId;
    }

    @Override
    public String name() {
        return name;
    }

    public String diskId() {
        return diskId;
    }

    @Override
    public String toString() {
        return "DiskVolumeRequest{" +
            "name='" + name + '\'' +
            ", diskId='" + diskId + '\'' +
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
        return name.equals(that.name) && diskId.equals(that.diskId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, diskId);
    }
}
