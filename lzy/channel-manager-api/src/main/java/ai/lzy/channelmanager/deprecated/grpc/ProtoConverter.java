package ai.lzy.channelmanager.deprecated.grpc;

import ai.lzy.model.DataScheme;
import ai.lzy.v1.channel.deprecated.LCM;
import ai.lzy.v1.channel.deprecated.LCMPS;

@Deprecated
public class ProtoConverter {

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
