package ai.lzy.channelmanager.channel;

import ai.lzy.channelmanager.control.ChannelController;
import ai.lzy.channelmanager.graph.LocalChannelGraph;
import ai.lzy.model.SlotStatus;
import ai.lzy.model.channel.ChannelSpec;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.stream.Stream;

public interface Channel {
    String id();
    String ownerWorkflowId();
    String name();
    ChannelSpec spec();
    Stream<SlotStatus> slotsStatus();

    void bind(Endpoint endpoint) throws ChannelException;
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
