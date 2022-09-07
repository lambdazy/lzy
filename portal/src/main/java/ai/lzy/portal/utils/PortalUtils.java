package ai.lzy.portal.utils;

import ai.lzy.fs.fs.LzyInputSlot;
import ai.lzy.fs.fs.LzyOutputSlot;
import ai.lzy.fs.fs.LzySlot;
import ai.lzy.model.deprecated.GrpcConverter;
import ai.lzy.model.grpc.ProtoConverter;
import ai.lzy.v1.LzyPortalApi;
import ai.lzy.v1.LzyPortalApi.PortalSlotStatus;

import java.net.URI;
import java.util.Optional;

public class PortalUtils {
    private PortalUtils() {}

    private static LzyPortalApi.PortalSlotStatus.Builder commonSlotStatusBuilder(LzySlot slot) {
        return LzyPortalApi.PortalSlotStatus.newBuilder()
            .setSlot(ProtoConverter.toProto(slot.definition()))
            .setState(slot.state());
    }

    public static PortalSlotStatus buildInputSlotStatus(LzyInputSlot slot) {
        return commonSlotStatusBuilder(slot)
            .setConnectedTo(Optional.ofNullable(slot.connectedTo()).map(URI::toString).orElse(""))
            .build();
    }

    public static PortalSlotStatus buildOutputSlotStatus(LzyOutputSlot slot) {
        return commonSlotStatusBuilder(slot)
            .setConnectedTo("")
            .build();
    }
}
