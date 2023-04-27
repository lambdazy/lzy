package ai.lzy.service.data;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import jakarta.annotation.Nullable;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
@JsonSerialize
@JsonDeserialize
public record KafkaTopicDesc(
    String username,
    String password,  // TODO: encrypt
    String topicName,
    @Nullable String sinkTaskId  // null for topics without sink
) { }
