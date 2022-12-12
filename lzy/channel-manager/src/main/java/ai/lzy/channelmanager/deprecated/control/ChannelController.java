package ai.lzy.channelmanager.deprecated.control;

import ai.lzy.channelmanager.deprecated.channel.ChannelException;
import ai.lzy.channelmanager.deprecated.channel.Endpoint;
import ai.lzy.channelmanager.deprecated.graph.ChannelGraph;

import java.util.stream.Stream;

public interface ChannelController {
    Stream<Endpoint> executeBind(ChannelGraph channelGraph, Endpoint slot) throws ChannelException;

    void executeUnBind(ChannelGraph channelGraph, Endpoint slot) throws ChannelException;

    void executeDestroy(ChannelGraph channelGraph) throws ChannelException;
}
