package ai.lzy.server;

import ai.lzy.server.channel.Channel;
import ai.lzy.server.channel.ChannelException;
import ai.lzy.server.channel.Endpoint;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import ru.yandex.cloud.ml.platform.lzy.model.SlotStatus;
import ru.yandex.cloud.ml.platform.lzy.model.channel.ChannelSpec;

public interface ChannelsManager {
    ChannelSpec get(String cid);

    Channel create(ChannelSpec spec);

    void bind(ChannelSpec channel, Endpoint endpoint) throws ChannelException;

    void unbind(ChannelSpec channel, Endpoint endpoint) throws ChannelException;

    void destroy(ChannelSpec channel);

    @Nullable
    ChannelSpec bound(Endpoint endpoint);

    void unbindAll(String sessionId);

    SlotStatus[] connected(ChannelSpec channel);

    Stream<ChannelSpec> channels();
}
