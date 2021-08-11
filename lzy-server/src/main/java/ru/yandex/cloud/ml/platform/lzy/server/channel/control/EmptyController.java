package ru.yandex.cloud.ml.platform.lzy.server.channel.control;

import ru.yandex.cloud.ml.platform.lzy.server.channel.ChannelController;
import ru.yandex.cloud.ml.platform.lzy.server.channel.ChannelException;
import ru.yandex.cloud.ml.platform.lzy.server.local.Binding;

public class EmptyController implements ChannelController {
    @Override
    public ChannelController executeBind(Binding slot) throws ChannelException {
        throw new ChannelException("Destroyed channel");
    }

    @Override
    public ChannelController executeUnBind(Binding slot) throws ChannelException {
        throw new ChannelException("Destroyed channel");
    }

    @Override
    public void executeDestroy() throws ChannelException {
        throw new ChannelException("Destroyed channel");
    }
}
