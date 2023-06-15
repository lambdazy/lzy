package ai.lzy.portal.grpc;

import ai.lzy.fs.fs.LzyInputSlot;
import ai.lzy.fs.fs.LzyOutputSlot;
import ai.lzy.fs.fs.LzySlot;
import ai.lzy.model.DataScheme;
import ai.lzy.portal.slots.SnapshotSlot;
import ai.lzy.v1.common.LMD;
import ai.lzy.v1.common.LMS;
import ai.lzy.v1.common.LMST;
import ai.lzy.v1.portal.LzyPortal;
import ai.lzy.v1.portal.LzyPortalApi;
import jakarta.annotation.Nullable;

import java.net.URI;
import java.util.Optional;

import static ai.lzy.model.grpc.ProtoConverter.toProto;

public enum ProtoConverter {
    ;

    public static LzyPortal.PortalSlotDesc makePortalOutputSlot(String slotUri, String slotName, String channelId,
                                                                @Nullable LMD.DataScheme dataScheme,
                                                                LMST.StorageConfig storageConfig)
    {
        return makePortalSlot(slotUri, slotName, channelId, dataScheme, LMS.Slot.Direction.OUTPUT, storageConfig);
    }

    public static LzyPortal.PortalSlotDesc makePortalInputSlot(String slotUri, String slotName, String channelId,
                                                               @Nullable LMD.DataScheme dataScheme,
                                                               LMST.StorageConfig storageConfig)
    {
        return makePortalSlot(slotUri, slotName, channelId, dataScheme, LMS.Slot.Direction.INPUT, storageConfig);
    }

    public static LzyPortal.PortalSlotDesc makePortalSlot(String slotUri, String slotName, String channelId,
                                                          @Nullable LMD.DataScheme dataScheme,
                                                          LMS.Slot.Direction direction,
                                                          LMST.StorageConfig storageConfig)
    {
        final LMST.StorageConfig.Builder builder = LMST.StorageConfig.newBuilder()
            .setUri(slotUri);
        if (storageConfig.hasS3()) {
            builder.setS3(storageConfig.getS3());
        } else if (storageConfig.hasAzure()) {
            builder.setAzure(storageConfig.getAzure());
        }

        return LzyPortal.PortalSlotDesc.newBuilder()
            .setSlot(LMS.Slot.newBuilder()
                .setName(slotName)
                .setMedia(LMS.Slot.Media.FILE)
                .setDirection(direction)
                .setContentType(dataScheme != null ? dataScheme : toProto(DataScheme.PLAIN)))
            .setChannelId(channelId)
            .setSnapshot(LzyPortal.PortalSlotDesc.Snapshot.newBuilder().setStorageConfig(builder.build()).build())
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
}
