package ai.lzy.portal.grpc;

import ai.lzy.fs.fs.LzyInputSlot;
import ai.lzy.fs.fs.LzyOutputSlot;
import ai.lzy.fs.fs.LzySlot;
import ai.lzy.model.DataScheme;
import ai.lzy.portal.slots.SnapshotSlot;
import ai.lzy.v1.common.LMS;
import ai.lzy.v1.common.LMS3;
import ai.lzy.v1.portal.LzyPortal;
import ai.lzy.v1.portal.LzyPortalApi;
import ai.lzy.v1.workflow.LWF.DataDescription.WhiteboardRef;

import java.net.URI;
import java.util.Optional;
import javax.annotation.Nullable;

import static ai.lzy.model.grpc.ProtoConverter.toProto;

public enum ProtoConverter {
    ;

    public static LzyPortal.PortalSlotDesc makePortalOutputSlot(String slotUri, String slotName,
                                                                String channelId, LMS3.S3Locator s3Locator)
    {
        return makePortalSlot(slotUri, slotName, channelId, LMS.Slot.Direction.OUTPUT, s3Locator, null);
    }

    public static LzyPortal.PortalSlotDesc makePortalInputSlot(String slotUri, String slotName,
                                                               String channelId, LMS3.S3Locator s3Locator,
                                                               @Nullable WhiteboardRef whiteboardRef)
    {
        return makePortalSlot(slotUri, slotName, channelId, LMS.Slot.Direction.INPUT, s3Locator, whiteboardRef);
    }

    public static LzyPortal.PortalSlotDesc makePortalSlot(String slotUri, String slotName, String channelId,
                                                          LMS.Slot.Direction direction, LMS3.S3Locator s3Locator,
                                                          @Nullable WhiteboardRef whiteboardRef)
    {
        var keyAndBucket = parseStorageUri(slotUri);
        var key = keyAndBucket[0];
        var bucket = keyAndBucket[1];

        var snapshot = LzyPortal.PortalSlotDesc.Snapshot.newBuilder()
            .setS3(LMS3.S3Locator.newBuilder()
                .setKey(key)
                .setBucket(bucket)
                .setAmazon(s3Locator.getAmazon()));

        if (whiteboardRef != null) {
            snapshot.setWhiteboardRef(LzyPortal.PortalSlotDesc.Snapshot.WhiteboardRef.newBuilder()
                .setWhiteboardId(whiteboardRef.getWhiteboardId())
                .setFieldName(whiteboardRef.getFieldName()));
        }

        return LzyPortal.PortalSlotDesc.newBuilder()
            .setSlot(LMS.Slot.newBuilder()
                .setName(slotName)
                .setMedia(LMS.Slot.Media.FILE)
                .setDirection(direction)
                .setContentType(toProto(DataScheme.PLAIN)))
            .setChannelId(channelId)
            .setSnapshot(snapshot)
            .build();
    }

    private static String[] parseStorageUri(String storageUri) {
        String[] schemaAndRest = storageUri.split("//", 2);
        return schemaAndRest[1].split("/", 2);
    }

    public static LzyPortal.PortalSlotDesc makePortalInputStdoutSlot(String taskId,
                                                                     String stdSlotName,
                                                                     String channelId)
    {
        return makePortalStdSlot(taskId, stdSlotName, channelId, LMS.Slot.Direction.INPUT, true);
    }

    public static LzyPortal.PortalSlotDesc makePortalInputStderrSlot(String taskId, String stdSlotName,
                                                                     String channelId)
    {
        return makePortalStdSlot(taskId, stdSlotName, channelId, LMS.Slot.Direction.INPUT, false);
    }

    public static LzyPortal.PortalSlotDesc makePortalStdSlot(String taskId, String stdSlotName, String channelId,
                                                             LMS.Slot.Direction direction, boolean isStdOut)
    {
        var slot = LzyPortal.PortalSlotDesc.newBuilder()
            .setSlot(LMS.Slot.newBuilder()
                .setName(stdSlotName)
                .setMedia(LMS.Slot.Media.FILE)
                .setDirection(direction)
                .setContentType(toProto(DataScheme.PLAIN))
                .build())
            .setChannelId(channelId);

        if (isStdOut) {
            slot.setStdout(LzyPortal.PortalSlotDesc.StdOut.newBuilder().setTaskId(taskId).build());
        } else {
            slot.setStderr(LzyPortal.PortalSlotDesc.StdErr.newBuilder().setTaskId(taskId).build());
        }

        return slot.build();
    }

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
        return uriSchema + "://" + s3locator.getKey() + "/" + s3locator.getBucket();
    }
}
