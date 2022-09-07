package ai.lzy.channelmanager.channel;

import ai.lzy.channelmanager.control.ChannelController;
import ai.lzy.model.basic.SlotStatus;
import java.util.stream.Stream;

public interface Channel {
    String id();
    String workflowId();
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
