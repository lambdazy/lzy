package ai.lzy.site;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Duration;

public record Cookie(
    @JsonProperty(value = "user_id")
    String userId,

    @JsonProperty(value = "session_id")
    String sessionId,

    @JsonIgnore
    Duration ttl
) {
    @JsonProperty(value = "max_age")
    String maxAge() {
        return Long.toString(ttl.getSeconds());
    }
}

