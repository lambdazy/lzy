package ru.yandex.cloud.ml.platform.lzy.server;

import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.data.DataSchema;
import ru.yandex.cloud.ml.platform.lzy.server.channel.Channel;
import ru.yandex.cloud.ml.platform.lzy.server.channel.ChannelException;
import ru.yandex.cloud.ml.platform.lzy.server.task.SlotStatus;
import ru.yandex.cloud.ml.platform.lzy.server.task.Task;

import javax.annotation.Nullable;
import java.util.UUID;
import java.util.stream.Stream;

public interface ChannelsRepository {
    Channel get(UUID cid);
    Channel create(DataSchema contentTypeFrom);
    void bind(Channel channel, Task servant, Slot slotName) throws ChannelException;
    void unbind(Channel channel, Task servant, Slot slotName) throws ChannelException;
    void destroy(Channel channel);

    @Nullable
    Channel bound(Task servant, Slot slotName);

    SlotStatus[] connected(Channel channel);
    Stream<Channel> channels();
}
