package ai.lzy.portal.grpc;

import ai.lzy.fs.fs.LzyInputSlot;
import ai.lzy.fs.fs.LzyOutputSlot;
import ai.lzy.fs.fs.LzySlot;
import ai.lzy.model.DataScheme;
import ai.lzy.v1.common.LMS;
import ai.lzy.v1.common.LMS3;
import ai.lzy.v1.portal.LzyPortal;
import ai.lzy.v1.portal.LzyPortalApi;

import java.net.URI;
import java.util.Optional;

import static ai.lzy.model.grpc.ProtoConverter.toProto;

public enum ProtoConverter {
    ;

    public static LzyPortal.PortalSlotDesc makePortalOutputSlot(String slotUri, String slotName,
                                                                String channelId, LMS3.S3Locator s3Locator)
    {
        return makePortalSlot(slotUri, slotName, channelId, LMS.Slot.Direction.OUTPUT, s3Locator);
    }

    public static LzyPortal.PortalSlotDesc makePortalInputSlot(String slotUri, String slotName,
                                                               String channelId, LMS3.S3Locator s3Locator)
    {
        return makePortalSlot(slotUri, slotName, channelId, LMS.Slot.Direction.INPUT, s3Locator);
    }

    public static LzyPortal.PortalSlotDesc makePortalSlot(String slotUri, String slotName, String channelId,
                                                          LMS.Slot.Direction direction, LMS3.S3Locator s3Locator)
    {
        return LzyPortal.PortalSlotDesc.newBuilder()
            .setSnapshot(LzyPortal.PortalSlotDesc.Snapshot.newBuilder()
                .setS3(LMS3.S3Locator.newBuilder()
                    .setKey(slotUri)
                    .setBucket(s3Locator.getBucket())
                    .setAmazon(s3Locator.getAmazon())))
            .setSlot(LMS.Slot.newBuilder()
                .setName(slotName)
                .setMedia(LMS.Slot.Media.FILE)
                .setDirection(direction)
                .setContentType(ai.lzy.model.grpc.ProtoConverter.toProto(DataScheme.PLAIN))
                .build())
            .setChannelId(channelId)
            .build();
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
        return LzyPortalApi.PortalSlotStatus.newBuilder()
            .setSlot(ai.lzy.model.grpc.ProtoConverter.toProto(slot.definition()))
            .setState(slot.state());
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
}
