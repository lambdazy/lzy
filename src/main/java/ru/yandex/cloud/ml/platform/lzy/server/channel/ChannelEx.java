package ru.yandex.cloud.ml.platform.lzy.server.channel;

import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.server.local.Binding;
import ru.yandex.cloud.ml.platform.lzy.server.task.Task;

import java.util.stream.Stream;

public interface ChannelEx extends Channel {
    void bind(Task task, Slot slot) throws ChannelException;
    void unbind(Task servant, Slot slot) throws ChannelException;
    void close();
    ChannelController controller();

    Stream<Binding> bound();
}
