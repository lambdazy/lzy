package ai.lzy.channelmanager.grpc;

import ai.lzy.channelmanager.channel.ChannelSpec;
import ai.lzy.channelmanager.channel.DirectChannelSpec;
import ai.lzy.channelmanager.channel.v2.Channel;
import ai.lzy.model.DataScheme;
import ai.lzy.v1.channel.v2.LCM;
import ai.lzy.v1.channel.v2.LCMPS;

import java.util.List;
import java.util.stream.Collectors;

public class ProtoConverter {

    public static ChannelSpec fromProto(LCM.ChannelSpec channel) {
        return new DirectChannelSpec(
            channel.getChannelName(),
            ai.lzy.model.grpc.ProtoConverter.fromProto(channel.getScheme())
        );
    }

    public static LCM.Channel toProto(Channel channel) {
        return LCM.Channel.newBuilder()
            .setChannelId(channel.id())
            .setSpec(toProto(channel.spec()))
            .setExecutionId(channel.executionId())
            .build();
        // TODO src, dst
    }

    public static LCM.ChannelSpec toProto(ChannelSpec channelSpec) {
        return LCM.ChannelSpec.newBuilder()
            .setChannelName(channelSpec.name())
            .setScheme(ai.lzy.model.grpc.ProtoConverter.toProto(channelSpec.contentType()))
            .build();
    }

    public static LCMPS.ChannelStatusResponse createChannelStatusResponse(Channel channel) {
        return LCMPS.ChannelStatusResponse.newBuilder()
            .setStatus(LCMPS.ChannelStatus.newBuilder()
                .setChannel(ProtoConverter.toProto(channel))
                .build())
            .build();
    }

    public static LCMPS.ChannelStatusAllResponse createChannelStatusAllResponse(List<Channel> channels) {
        return LCMPS.ChannelStatusAllResponse.newBuilder()
            .addAllStatuses(channels.stream()
                .map(channel -> LCMPS.ChannelStatus.newBuilder()
                    .setChannel(ProtoConverter.toProto(channel))
                    .build())
                .collect(Collectors.toList()))
            .build();
    }

    public static ai.lzy.v1.channel.LCMPS.ChannelCreateRequest makeCreateDirectChannelCommand(String workflowId, String channelName) {
        return ai.lzy.v1.channel.LCMPS.ChannelCreateRequest.newBuilder()
            .setExecutionId(workflowId)
            .setChannelSpec(
                ai.lzy.v1.channel.LCM.ChannelSpec.newBuilder()
                    .setChannelName(channelName)
                    .setContentType(ai.lzy.model.grpc.ProtoConverter.toProto(DataScheme.PLAIN))
                    .setDirect(ai.lzy.v1.channel.LCM.DirectChannelType.getDefaultInstance())
                    .build()
            ).build();
    }

    public static ai.lzy.v1.channel.LCMPS.ChannelDestroyRequest makeDestroyChannelCommand(String channelId) {
        return ai.lzy.v1.channel.LCMPS.ChannelDestroyRequest.newBuilder()
            .setChannelId(channelId)
            .build();
    }

    public static ai.lzy.v1.channel.LCMPS.ChannelDestroyAllRequest makeDestroyAllCommand(String workflowId) {
        return ai.lzy.v1.channel.LCMPS.ChannelDestroyAllRequest.newBuilder().setExecutionId(workflowId).build();
    }
}
