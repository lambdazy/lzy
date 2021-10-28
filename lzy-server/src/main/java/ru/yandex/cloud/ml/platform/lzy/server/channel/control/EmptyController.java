package ru.yandex.cloud.ml.platform.lzy.server.channel.control;

import ru.yandex.cloud.ml.platform.lzy.server.channel.ChannelController;
import ru.yandex.cloud.ml.platform.lzy.server.channel.ChannelException;
import ru.yandex.cloud.ml.platform.lzy.server.channel.ChannelGraph;
import ru.yandex.cloud.ml.platform.lzy.server.channel.Endpoint;

public class EmptyController implements ChannelController {
    @Override
    public void executeBind(
        ChannelGraph channelGraph, Endpoint slot
    ) throws ChannelException {
        throw new ChannelException("Destroyed channel");
    }

    @Override
    public void executeUnBind(
        ChannelGraph channelGraph, Endpoint slot
    ) throws ChannelException {
        throw new ChannelException("Destroyed channel");
    }

    @Override
    public void executeDestroy(ChannelGraph channelGraph) throws ChannelException {
        throw new ChannelException("Destroyed channel");
    }
}
