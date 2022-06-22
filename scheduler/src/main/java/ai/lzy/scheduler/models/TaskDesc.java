package ai.lzy.scheduler.models;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Map;
import ru.yandex.cloud.ml.platform.lzy.model.graph.AtomicZygote;
import ru.yandex.cloud.ml.platform.lzy.model.json.ZygoteDeserializer;
import ru.yandex.cloud.ml.platform.lzy.model.json.ZygoteSerializer;

public record TaskDesc(
    @JsonSerialize(using = ZygoteSerializer.class)
    @JsonDeserialize(using = ZygoteDeserializer.class)
    AtomicZygote zygote,
    Map<String, String> slotsToChannelsAssignments
) { }
