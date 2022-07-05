package ai.lzy.scheduler.models;

import ai.lzy.model.GrpcConverter;
import ai.lzy.model.graph.AtomicZygote;
import ai.lzy.model.json.ZygoteDeserializer;
import ai.lzy.model.json.ZygoteSerializer;
import ai.lzy.priv.v2.SchedulerApi;
import ai.lzy.priv.v2.SchedulerApi.SlotToChannelAssignment;
import ai.lzy.priv.v2.SchedulerGrpc;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.grpc.Status;

import java.util.Map;
import java.util.stream.Collectors;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
@JsonSerialize
@JsonDeserialize
public record TaskDesc(
    @JsonSerialize(using = ZygoteSerializer.class)
    @JsonDeserialize(using = ZygoteDeserializer.class)
    AtomicZygote zygote,
    Map<String, String> slotsToChannelsAssignments
) {
    public static TaskDesc fromGrpc(SchedulerApi.TaskDesc taskDesc) {
        Map<String, String> slotMapping = taskDesc.getSlotAssignmentsList()
            .stream()
            .collect(Collectors.toMap(SlotToChannelAssignment::getSlotName, SlotToChannelAssignment::getChannelId));
        var zygote = GrpcConverter.from(taskDesc.getZygote());
        if (zygote instanceof AtomicZygote) {
            return new TaskDesc((AtomicZygote) zygote, slotMapping);
        }
        throw Status.INVALID_ARGUMENT.withDescription("Method applies only atomic zygotes").asRuntimeException();
    }
}
