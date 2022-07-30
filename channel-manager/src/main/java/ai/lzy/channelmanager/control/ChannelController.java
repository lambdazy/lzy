package ai.lzy.channelmanager.control;

import ai.lzy.channelmanager.channel.ChannelException;
import ai.lzy.channelmanager.channel.Endpoint;
import ai.lzy.channelmanager.graph.ChannelGraph;

public interface ChannelController {
    void executeBind(ChannelGraph channelGraph, Endpoint slot) throws ChannelException;

    void executeUnBind(ChannelGraph channelGraph, Endpoint slot) throws ChannelException;

    void executeDestroy(ChannelGraph channelGraph) throws ChannelException;
}
