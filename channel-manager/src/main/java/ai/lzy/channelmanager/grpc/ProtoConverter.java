package ai.lzy.channelmanager.grpc;

import ai.lzy.channelmanager.channel.Channel;
import ai.lzy.channelmanager.channel.ChannelSpec;
import ai.lzy.v1.channel.LCMS;
import ai.lzy.v1.channel.LCM;
import ai.lzy.v1.common.LMS;
import ai.lzy.v1.deprecated.LzyZygote;

public class ProtoConverter {

    public static LCMS.ChannelStatus toChannelStatusProto(Channel channel) {
        final LCMS.ChannelStatus.Builder statusBuilder = LCMS.ChannelStatus.newBuilder();
        statusBuilder
            .setChannelId(channel.id())
            .setChannelSpec(toProto(channel.spec()));
        channel.slotsStatus()
            .map(slotStatus ->
                LMS.SlotStatus.newBuilder()
                    .setTaskId(slotStatus.tid())
                    .setConnectedTo(channel.id())
                    .setDeclaration(ai.lzy.model.grpc.ProtoConverter.toProto(slotStatus.slot()))
                    .setPointer(slotStatus.pointer())
                    .setState(LMS.SlotStatus.State.valueOf(slotStatus.state().toString()))
                    .build())
            .forEach(statusBuilder::addConnected);
        return statusBuilder.build();
    }

    public static LCM.ChannelSpec toProto(ChannelSpec channel) {
        final LCM.ChannelSpec.Builder builder = LCM.ChannelSpec.newBuilder();
        builder.setChannelName(channel.name());
        builder.setContentType(ai.lzy.model.grpc.ProtoConverter.toProto(channel.contentType()));
        return builder.build();
    }

}
