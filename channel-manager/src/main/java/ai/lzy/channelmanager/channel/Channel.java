package ai.lzy.channelmanager.channel;

import ai.lzy.channelmanager.control.ChannelController;
import ai.lzy.model.SlotStatus;
import ai.lzy.model.channel.ChannelSpec;
import java.util.stream.Stream;

public interface Channel {
    String id();
    String name();
    ChannelSpec spec();
    String workflowId();

    void bind(Endpoint endpoint) throws ChannelException;
    void unbind(Endpoint endpoint) throws ChannelException;

    void close();

    ChannelController controller();

    Stream<Endpoint> bound();

    boolean hasBound(Endpoint endpoint);

    Stream<SlotStatus> slotsStatus();
}
