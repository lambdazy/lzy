package ai.lzy.server.channel.control;

import ai.lzy.server.channel.ChannelController;
import ai.lzy.server.channel.ChannelException;
import ai.lzy.server.channel.ChannelGraph;
import ai.lzy.server.channel.Endpoint;

public class EmptyController implements ChannelController {
    @Override
    public void executeBind(ChannelGraph channelGraph, Endpoint slot) throws ChannelException {
        throw new ChannelException("Destroyed channel for slot: " + slot.uri());
    }

    @Override
    public void executeUnBind(ChannelGraph channelGraph, Endpoint slot) throws ChannelException {
        throw new ChannelException("Destroyed channel for slot: " + slot.uri());
    }

    @Override
    public void executeDestroy(ChannelGraph channelGraph) throws ChannelException {
        throw new ChannelException("Destroyed channel");
    }
}
