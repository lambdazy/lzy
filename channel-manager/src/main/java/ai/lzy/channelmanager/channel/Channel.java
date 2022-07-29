package ai.lzy.channelmanager.channel;

import ai.lzy.model.SlotStatus;
import ai.lzy.model.channel.ChannelSpec;
import java.util.stream.Stream;

public interface Channel {
    String id();
    String name();
    ChannelSpec spec();
    Stream<SlotStatus> slotsStatus();

    void bind(Endpoint endpoint) throws ChannelException;
    void unbind(Endpoint endpoint) throws ChannelException;
    Stream<Endpoint> bound();
    boolean hasBound(Endpoint endpoint);

    void close();
}
