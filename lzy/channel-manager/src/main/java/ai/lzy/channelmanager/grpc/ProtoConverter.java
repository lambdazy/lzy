package ai.lzy.channelmanager.grpc;

import ai.lzy.channelmanager.model.channel.Channel;
import ai.lzy.channelmanager.model.channel.Receivers;
import ai.lzy.channelmanager.model.channel.Senders;
import ai.lzy.v1.channel.LCM;


public class ProtoConverter {

    public static LCM.Channel toProto(Channel channel) {
        Senders activeSenders = channel.getActiveSenders();
        LCM.ChannelSenders.Builder sendersBuilder = LCM.ChannelSenders.newBuilder();
        if (activeSenders.portalEndpoint() != null) {
            sendersBuilder.setPortalSlot(
                ai.lzy.model.grpc.ProtoConverter.toProto(activeSenders.portalEndpoint().getSlot()));
        }
        if (activeSenders.workerEndpoint() != null) {
            sendersBuilder.setWorkerSlot(
                ai.lzy.model.grpc.ProtoConverter.toProto(activeSenders.workerEndpoint().getSlot()));
        }

        Receivers activeReceivers = channel.getActiveReceivers();
        LCM.ChannelReceivers.Builder receiversBuilder = LCM.ChannelReceivers.newBuilder();
        if (activeReceivers.portalEndpoint() != null) {
            receiversBuilder.setPortalSlot(
                ai.lzy.model.grpc.ProtoConverter.toProto(activeReceivers.portalEndpoint().getSlot()));
        }
        receiversBuilder.addAllWorkerSlots(activeReceivers.workerEndpoints().stream()
            .map(e -> ai.lzy.model.grpc.ProtoConverter.toProto(e.getSlot()))
            .toList());

        return LCM.Channel.newBuilder()
            .setChannelId(channel.getId())
            .setSpec(toProto(channel.getSpec()))
            .setExecutionId(channel.getExecutionId())
            .setSenders(sendersBuilder.build())
            .setReceivers(receiversBuilder.build())
            .build();
    }

    public static LCM.ChannelSpec toProto(Channel.Spec channelSpec) {
        return LCM.ChannelSpec.newBuilder()
            .setChannelName(channelSpec.name())
            .setScheme(ai.lzy.model.grpc.ProtoConverter.toProto(channelSpec.contentType()))
            .build();
    }

}
