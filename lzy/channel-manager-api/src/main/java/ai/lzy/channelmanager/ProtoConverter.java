package ai.lzy.channelmanager;

import ai.lzy.model.DataScheme;
import ai.lzy.model.slot.SlotInstance;
import ai.lzy.v1.channel.LCM;
import ai.lzy.v1.channel.LCMPS;
import ai.lzy.v1.channel.LCMS;

import java.net.URI;

import static ai.lzy.model.grpc.ProtoConverter.toProto;
import static ai.lzy.v1.channel.LCMS.BindRequest.SlotOwner.PORTAL;
import static ai.lzy.v1.channel.LCMS.BindRequest.SlotOwner.WORKER;

public class ProtoConverter {

    public static LCMPS.ChannelCreateRequest makeCreateChannelCommand(
        String userId, String workflowName, String executionId, String channelName)
    {
        return LCMPS.ChannelCreateRequest.newBuilder()
            .setUserId(userId)
            .setWorkflowName(workflowName)
            .setExecutionId(executionId)
            .setChannelSpec(
                LCM.ChannelSpec.newBuilder()
                    .setChannelName(channelName)
                    .setScheme(toProto(DataScheme.PLAIN))
                    .build()
            ).build();
    }

    public static LCMPS.ChannelDestroyRequest makeDestroyChannelCommand(String channelId) {
        return LCMPS.ChannelDestroyRequest.newBuilder().setChannelId(channelId).build();
    }

    public static LCMPS.ChannelStatusRequest makeChannelStatusCommand(String channelId) {
        return LCMPS.ChannelStatusRequest.newBuilder().setChannelId(channelId).build();
    }

    public static LCMS.BindRequest makeBindSlotCommand(SlotInstance slotInstance, boolean ownerIsPortal) {
        return LCMS.BindRequest.newBuilder()
            .setSlotOwner(ownerIsPortal ? PORTAL : WORKER)
            .setSlotInstance(toProto(slotInstance))
            .build();
    }

    public static LCMS.UnbindRequest makeUnbindSlotCommand(URI slotUri) {
        return LCMS.UnbindRequest.newBuilder().setSlotUri(slotUri.toString()).build();
    }

}
