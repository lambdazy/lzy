package ru.yandex.cloud.ml.platform.lzy.server.channel;

import ru.yandex.cloud.ml.platform.lzy.model.Channel;

import java.util.Map;
import java.util.stream.Stream;

public interface ChannelEx extends Channel {
    void bind(Endpoint endpoint) throws ChannelException;
    void unbind(Endpoint endpoint) throws ChannelException;
    void close();
    ChannelController controller();

    Stream<Endpoint> bound();
    boolean hasBound(Endpoint endpoint);
    boolean isPersistent();
    void addLinkToStorage(String slotName, String linkToStorage);
    Map<String, String> getLinksToStorage();
}
