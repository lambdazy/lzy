package ai.lzy.allocator.volume;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

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
}
