package ru.yandex.cloud.ml.platform.lzy.server.channel;

import java.util.stream.Stream;
import ru.yandex.cloud.ml.platform.lzy.model.channel.Channel;

public interface ChannelEx extends Channel {
    void bind(Endpoint endpoint) throws ChannelException;

    void unbind(Endpoint endpoint) throws ChannelException;

    void close();

    ChannelController controller();

    Stream<Endpoint> bound();

    boolean hasBound(Endpoint endpoint);
}
