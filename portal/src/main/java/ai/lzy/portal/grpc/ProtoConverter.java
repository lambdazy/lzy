package ai.lzy.portal.grpc;

import ai.lzy.fs.fs.LzyInputSlot;
import ai.lzy.fs.fs.LzyOutputSlot;
import ai.lzy.fs.fs.LzySlot;
import ai.lzy.portal.slots.SnapshotSlot;
import ai.lzy.v1.common.LMS3;
import ai.lzy.v1.portal.LzyPortalApi;

import java.net.URI;
import java.util.Optional;

public enum ProtoConverter {
    ;

    private static LzyPortalApi.PortalSlotStatus.Builder commonSlotStatusBuilder(LzySlot slot) {
        var builder = LzyPortalApi.PortalSlotStatus.newBuilder()
            .setSlot(ai.lzy.model.grpc.ProtoConverter.toProto(slot.definition()))
            .setState(slot.state());
        if (slot instanceof SnapshotSlot) {
            builder.setSnapshotStatus(
                switch (((SnapshotSlot) slot).snapshotState()) {
                    case INITIALIZING -> LzyPortalApi.PortalSlotStatus.SnapshotSlotStatus.INITIALIZING;
                    case SYNCING -> LzyPortalApi.PortalSlotStatus.SnapshotSlotStatus.SYNCING;
                    case SYNCED -> LzyPortalApi.PortalSlotStatus.SnapshotSlotStatus.SYNCED;
                    case FAILED -> LzyPortalApi.PortalSlotStatus.SnapshotSlotStatus.FAILED;
                }
            );
        } else {
            builder.setSnapshotStatus(LzyPortalApi.PortalSlotStatus.SnapshotSlotStatus.NOT_IN_SNAPSHOT);
        }
        return builder;
    }

    public static LzyPortalApi.PortalSlotStatus buildInputSlotStatus(LzyInputSlot slot) {
        return commonSlotStatusBuilder(slot)
            .setConnectedTo(Optional.ofNullable(slot.connectedTo()).map(URI::toString).orElse(""))
            .build();
    }

    public static LzyPortalApi.PortalSlotStatus buildOutputSlotStatus(LzyOutputSlot slot) {
        return commonSlotStatusBuilder(slot)
            .setConnectedTo("")
            .build();
    }

    public static String getSlotUri(LMS3.S3Locator s3locator) {
        var uriSchema = switch (s3locator.getEndpointCase()) {
            case AZURE -> "azure";
            case AMAZON -> "s3";
            case ENDPOINT_NOT_SET -> throw new IllegalArgumentException("Unsupported bucket storage type");
        };
        return uriSchema + "://" + s3locator.getBucket() + "/" + s3locator.getKey();
    }
}
