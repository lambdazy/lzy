package ai.lzy.channelmanager.deprecated.channel;

import ai.lzy.channelmanager.deprecated.control.ChannelController;
import ai.lzy.model.slot.SlotStatus;

import java.util.stream.Stream;

public interface Channel {
    String id();
    String executionId();
    String name();
    ChannelSpec spec();
    Stream<SlotStatus> slotsStatus();

    Stream<Endpoint> bind(Endpoint endpoint) throws ChannelException;
    void unbind(Endpoint endpoint) throws ChannelException;
    Stream<Endpoint> bound();
    Stream<Endpoint> bound(Endpoint endpoint);
    boolean hasBound(Endpoint endpoint);

    void destroy();

    interface Builder {

        Builder setId(String id);

        Builder setSpec(ChannelSpec spec);

        Builder setController(ChannelController controller);

        Builder addSender(Endpoint senderEndpoint);

        Builder addReceiver(Endpoint receiverEndpoint);

        Builder addEdge(String senderUri, String receiverUri);

        Channel build();

    }
}