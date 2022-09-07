package ai.lzy.model.operation;

import ai.lzy.model.graph.Env;
import ai.lzy.model.grpc.ProtoConverter;
import ai.lzy.model.slot.Slot;
import ai.lzy.v1.common.LzyCommon;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import javax.annotation.Nullable;
import java.util.List;


public record Operation(
    Env env,
    Requirements requirements,
    String command,
    List<Slot> slots,
    String description,
    String name,

    @Nullable StdSlotDesc stdout,
    @Nullable StdSlotDesc stderr
) {

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    @JsonSerialize
    @JsonDeserialize
    public record Requirements(
        String poolLabel,
        String zone
    ) { }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    @JsonSerialize
    @JsonDeserialize
    public record StdSlotDesc(
        String slotName,
        String channelId
    ){ }

    public static Operation fromProto(LzyCommon.Operation operation) {
        final var req = new Requirements(
            operation.getRequirements().getPoolLabel(),
            operation.getRequirements().getZone());

        StdSlotDesc stdout = null;
        if (operation.hasStdout()) {
            stdout = new StdSlotDesc(
                operation.getStdout().getName(),
                operation.getStdout().getChannelId()
            );
        }

        StdSlotDesc stderr = null;
        if (operation.hasStdout()) {
            stderr = new StdSlotDesc(
                operation.getStdout().getName(),
                operation.getStdout().getChannelId()
            );
        }

        return new Operation(
            ProtoConverter.fromProto(operation.getEnv()),
            req,
            operation.getCommand(),
            operation.getSlotsList()
                .stream()
                .map(q -> ProtoConverter.fromProto(q))
                .toList(),
            operation.getDescription(),
            operation.getName(),
            stdout,
            stderr
        );
    }

    public LzyCommon.Operation toProto() {
        final var req = LzyCommon.Requirements.newBuilder()
            .setPoolLabel(requirements.poolLabel)
            .setZone(requirements.zone)
            .build();

        final var builder =  LzyCommon.Operation.newBuilder()
            .setEnv(ProtoConverter.toProto(env))
            .setRequirements(req)
            .setCommand(command)
            .addAllSlots(slots.stream()
                .map(ProtoConverter::toProto)
                .toList())
            .setDescription(description)
            .setName(name);

        if (stdout != null) {
            builder.setStdout(LzyCommon.Operation.StdSlotDesc.newBuilder()
                .setChannelId(stdout.channelId)
                .setName(stdout.slotName)
                .build());
        }

        if (stderr != null) {
            builder.setStdout(LzyCommon.Operation.StdSlotDesc.newBuilder()
                .setChannelId(stderr.channelId)
                .setName(stderr.slotName)
                .build());
        }

        return builder.build();
    }
}
