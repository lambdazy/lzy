package ai.lzy.portal;

import ai.lzy.model.DataScheme;
import ai.lzy.v1.common.LMS;
import ai.lzy.v1.common.LMS3;
import ai.lzy.v1.portal.LzyPortal;
import ai.lzy.v1.portal.LzyPortal.PortalSlotDesc.Snapshot.WhiteboardRef;

import javax.annotation.Nullable;

import static ai.lzy.model.grpc.ProtoConverter.toProto;

public class Utils {
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
        var bucket = keyAndBucket[0];
        var key = keyAndBucket[1];

        var snapshot = LzyPortal.PortalSlotDesc.Snapshot.newBuilder()
            .setS3(LMS3.S3Locator.newBuilder()
                .setKey(key)
                .setBucket(bucket)
                .setAmazon(s3Locator.getAmazon()));

        if (whiteboardRef != null) {
            snapshot.setWhiteboardRef(WhiteboardRef.newBuilder()
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
}
