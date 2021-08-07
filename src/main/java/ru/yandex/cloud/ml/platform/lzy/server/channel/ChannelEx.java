package ru.yandex.cloud.ml.platform.lzy.server.channel;

import ru.yandex.cloud.ml.platform.lzy.server.local.Binding;

import java.util.stream.Stream;

public interface ChannelEx extends Channel {
    void bind(Binding binding) throws ChannelException;
    void unbind(Binding binding) throws ChannelException;
    void close();
    ChannelController controller();

    Stream<Binding> bound();
}
