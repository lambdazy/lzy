package ru.yandex.cloud.ml.platform.lzy.server;

import java.util.UUID;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import ru.yandex.cloud.ml.platform.lzy.model.SlotStatus;
import ru.yandex.cloud.ml.platform.lzy.model.channel.ChannelSpec;
import ru.yandex.cloud.ml.platform.lzy.server.channel.Channel;
import ru.yandex.cloud.ml.platform.lzy.server.channel.ChannelException;
import ru.yandex.cloud.ml.platform.lzy.server.channel.Endpoint;

public interface ChannelsManager {
    ChannelSpec get(String cid);

    Channel create(ChannelSpec spec);

    void bind(ChannelSpec channel, Endpoint endpoint) throws ChannelException;

    void unbind(ChannelSpec channel, Endpoint endpoint) throws ChannelException;

    void destroy(ChannelSpec channel);

    @Nullable
    ChannelSpec bound(Endpoint endpoint);

    void unbindAll(UUID sessionId);

    SlotStatus[] connected(ChannelSpec channel);

    Stream<ChannelSpec> channels();
}
