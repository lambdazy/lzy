package ai.lzy.scheduler.models;

import ai.lzy.model.graph.AtomicZygote;
import ai.lzy.model.json.ZygoteDeserializer;
import ai.lzy.model.json.ZygoteSerializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Map;

public record TaskDesc(
    @JsonSerialize(using = ZygoteSerializer.class)
    @JsonDeserialize(using = ZygoteDeserializer.class)
    AtomicZygote zygote,
    Map<String, String> slotsToChannelsAssignments
) { }
