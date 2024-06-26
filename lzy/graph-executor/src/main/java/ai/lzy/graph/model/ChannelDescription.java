package ai.lzy.graph.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
@JsonSerialize
@JsonDeserialize
public record ChannelDescription(Type type, String id) {
    public ChannelDescription(String id) {
        this(Type.DIRECT, id);
    }

    public enum Type {
        DIRECT
    }
}
