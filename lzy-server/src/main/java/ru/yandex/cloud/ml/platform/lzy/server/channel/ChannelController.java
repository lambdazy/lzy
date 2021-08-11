package ru.yandex.cloud.ml.platform.lzy.server.channel;

import ru.yandex.cloud.ml.platform.lzy.server.local.Binding;

public interface ChannelController {
    ChannelController executeBind(Binding slot) throws ChannelException;
    ChannelController executeUnBind(Binding slot) throws ChannelException;
    void executeDestroy() throws ChannelException;
}
