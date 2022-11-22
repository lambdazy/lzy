package ai.lzy.allocator.volume;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonSerialize
@JsonDeserialize
public record VolumeMount(
    @JsonInclude
    String name,
    @JsonInclude
    String path,
    @JsonInclude
    boolean readOnly,
    @JsonInclude
    MountPropagation mountPropagation
) {
    public enum MountPropagation {
        NONE("None"),
        HOST_TO_CONTAINER("HostToContainer"),
        BIDIRECTIONAL("Bidirectional");

        private final String repr;

        MountPropagation(String repr) {
            this.repr = repr;
        }

        public String asString() {
            return repr;
        }
    }
}
