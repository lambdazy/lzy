package ru.yandex.cloud.ml.platform.lzy.server;

import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.data.DataSchema;
import ru.yandex.cloud.ml.platform.lzy.server.channel.Channel;
import ru.yandex.cloud.ml.platform.lzy.server.channel.ChannelException;
import ru.yandex.cloud.ml.platform.lzy.server.local.Binding;
import ru.yandex.cloud.ml.platform.lzy.server.task.SlotStatus;
import ru.yandex.cloud.ml.platform.lzy.server.task.Task;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.Map;
import java.util.stream.Stream;

public interface ChannelsRepository {
    Channel get(String cid);
    Channel create(String name, DataSchema contentTypeFrom);
    void bind(Channel channel, Binding binding) throws ChannelException;
    void unbind(Channel channel, Binding binding) throws ChannelException;
    void destroy(Channel channel);

    @Nullable
    Channel bound(URI slotUri);
    void unbindAll(URI servantUri);

    SlotStatus[] connected(Channel channel);
    Stream<Channel> channels();
}
