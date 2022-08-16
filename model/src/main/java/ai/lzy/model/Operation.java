package ai.lzy.model;

import ai.lzy.model.graph.Env;
import ai.lzy.v1.common.LzyCommon;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.List;


public record Operation(
    Env env,
    Requirements requirements,
    String command,
    List<Slot> slots,
    String description,
    String name
) {

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    @JsonSerialize
    @JsonDeserialize
    public record Requirements(
        String poolLabel,
        String zone
    ) { }

    public static Operation from(LzyCommon.Operation operation) {
        final var req = new Requirements(
            operation.getRequirements().getPoolLabel(),
            operation.getRequirements().getZone());

        return new Operation(
            GrpcConverter.from(operation.getEnv()),
            req,
            operation.getCommand(),
            operation.getSlotsList()
                .stream()
                .map(GrpcConverter::from)
                .toList(),
            operation.getDescription(),
            operation.getName()
        );
    }

    public LzyCommon.Operation to() {
        final var req = LzyCommon.Requirements.newBuilder()
            .setPoolLabel(requirements.poolLabel)
            .setZone(requirements.zone)
            .build();

        return LzyCommon.Operation.newBuilder()
            .setEnv(GrpcConverter.to(env))
            .setRequirements(req)
            .setCommand(command)
            .addAllSlots(slots.stream()
                .map(GrpcConverter::to)
                .toList())
            .setDescription(description)
            .setName(name)
            .build();
    }
}
