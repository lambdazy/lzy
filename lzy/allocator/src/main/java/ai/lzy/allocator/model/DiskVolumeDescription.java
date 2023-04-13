package ai.lzy.allocator.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class DiskVolumeDescription extends VolumeRequest.ResourceVolumeDescription {
    private final String id;
    private final String diskId;
    private final int sizeGb;

    @JsonCreator
    public DiskVolumeDescription(@JsonProperty("id") String id,
                                 @JsonProperty("diskId") String diskId,
                                 @JsonProperty("sizeGb") int sizeGb)
    {
        this.id = id;
        this.diskId = diskId;
        this.sizeGb = sizeGb;
    }

    @Override
    public String id() {
        return id;
    }

    public String diskId() {
        return diskId;
    }

    public int sizeGb() {
        return sizeGb;
    }

    @Override
    public String toString() {
        return "DiskVolumeDescription{" +
            "id='" + id + '\'' +
            ", diskId='" + diskId + '\'' +
            ", sizeGb=" + sizeGb +
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
        return id.equals(that.id) && diskId.equals(that.diskId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, diskId);
    }
}
