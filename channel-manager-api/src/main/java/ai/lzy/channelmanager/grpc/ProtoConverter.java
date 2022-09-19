package ai.lzy.channelmanager.grpc;

import ai.lzy.model.DataScheme;
import ai.lzy.v1.channel.LCM;
import ai.lzy.v1.channel.LCMPS;

public class ProtoConverter {

    public static LCMPS.ChannelCreateRequest createChannelRequest(String workflowId, LCM.ChannelSpec spec) {
        return LCMPS.ChannelCreateRequest.newBuilder()
            .setExecutionId(workflowId)
            .setChannelSpec(spec)
            .build();
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
