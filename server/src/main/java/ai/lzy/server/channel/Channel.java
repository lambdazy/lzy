package ai.lzy.server.channel;

import java.util.stream.Stream;
import ai.lzy.model.channel.ChannelSpec;

public interface Channel extends ChannelSpec {
    void bind(Endpoint endpoint) throws ChannelException;
    void unbind(Endpoint endpoint) throws ChannelException;

    void close();

    ChannelController controller();

    Stream<Endpoint> bound();

    boolean hasBound(Endpoint endpoint);
}
