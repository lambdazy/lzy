package ai.lzy.channelmanager.grpc;

import ai.lzy.channelmanager.channel.Channel;
import ai.lzy.channelmanager.channel.ChannelSpec;
import ai.lzy.v1.ChannelManager;
import ai.lzy.v1.Channels;
import ai.lzy.v1.Operations;

public class ProtoConverter {

    public static ChannelManager.ChannelStatus toChannelStatusProto(Channel channel) {
        final ChannelManager.ChannelStatus.Builder statusBuilder = ChannelManager.ChannelStatus.newBuilder();
        statusBuilder
            .setChannelId(channel.id())
            .setChannelSpec(toProto(channel.spec()));
        channel.slotsStatus()
            .map(slotStatus ->
                Operations.SlotStatus.newBuilder()
                    .setTaskId(slotStatus.tid())
                    .setConnectedTo(channel.id())
                    .setDeclaration(ai.lzy.model.grpc.ProtoConverter.toProto(slotStatus.slot()))
                    .setPointer(slotStatus.pointer())
                    .setState(Operations.SlotStatus.State.valueOf(slotStatus.state().toString()))
                    .build())
            .forEach(statusBuilder::addConnected);
        return statusBuilder.build();
    }

    public static Channels.ChannelSpec toProto(ChannelSpec channel) {
        final Channels.ChannelSpec.Builder builder = Channels.ChannelSpec.newBuilder();
        builder.setChannelName(channel.name());
        builder.setContentType(ai.lzy.model.grpc.ProtoConverter.toProto(channel.contentType()));
        return builder.build();
    }

}
