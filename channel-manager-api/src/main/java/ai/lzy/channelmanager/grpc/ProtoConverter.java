package ai.lzy.channelmanager.grpc;

import ai.lzy.channelmanager.channel.ChannelSpec;
import ai.lzy.model.DataScheme;
import ai.lzy.v1.channel.LCM;
import ai.lzy.v1.channel.LCMPS;

public class ProtoConverter {

    public static LCM.ChannelSpec toProto(ChannelSpec channel) {
        final LCM.ChannelSpec.Builder builder = LCM.ChannelSpec.newBuilder();
        builder.setChannelName(channel.name());
        builder.setContentType(ai.lzy.model.grpc.ProtoConverter.toProto(channel.contentType()));
        return builder.build();
    }

    public static LCMPS.ChannelCreateRequest makeCreateDirectChannelCommand(String workflowId, String channelName) {
        return LCMPS.ChannelCreateRequest.newBuilder()
            .setExecutionId(workflowId)
            .setChannelSpec(
                LCM.ChannelSpec.newBuilder()
                    .setChannelName(channelName)
                    .setContentType(ai.lzy.model.grpc.ProtoConverter.toProto(DataScheme.PLAIN))
                    .setDirect(LCM.DirectChannelType.getDefaultInstance())
                    .build()
            ).build();
    }

    public static LCMPS.ChannelDestroyRequest makeDestroyChannelCommand(String channelId) {
        return LCMPS.ChannelDestroyRequest.newBuilder()
            .setChannelId(channelId)
            .build();
    }

    public static LCMPS.ChannelDestroyAllRequest makeDestroyAllCommand(String workflowId) {
        return LCMPS.ChannelDestroyAllRequest.newBuilder().setExecutionId(workflowId).build();
    }
}
