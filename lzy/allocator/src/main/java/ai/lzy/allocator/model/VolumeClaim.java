package ai.lzy.allocator.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
@JsonSerialize
@JsonDeserialize
public record VolumeClaim(
    String id,
    String clusterId,
    String name,
    Volume volume
) {
    public String volumeName() {
        return volume.name();
    }

    public Volume.AccessMode accessMode() {
        return volume.accessMode();
    }

    public String volumeRequestName() {
        return volume.volumeRequestName();
    }
}
