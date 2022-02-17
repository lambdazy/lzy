package ru.yandex.cloud.ml.platform.lzy.server;

import java.util.UUID;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import ru.yandex.cloud.ml.platform.lzy.model.Channel;
import ru.yandex.cloud.ml.platform.lzy.model.SlotStatus;
import ru.yandex.cloud.ml.platform.lzy.model.data.DataSchema;
import ru.yandex.cloud.ml.platform.lzy.server.channel.ChannelException;
import ru.yandex.cloud.ml.platform.lzy.server.channel.Endpoint;

public interface ChannelsManager {
    Channel get(String cid);

    Channel create(String name, DataSchema contentTypeFrom);

    void bind(Channel channel, Endpoint endpoint) throws ChannelException;

    void unbind(Channel channel, Endpoint endpoint) throws ChannelException;

    void destroy(Channel channel);

    @Nullable
    Channel bound(Endpoint endpoint);

    void unbindAll(UUID sessionId);

    SlotStatus[] connected(Channel channel);

    Stream<Channel> channels();
}
