package ai.lzy.channelmanager.control;

import ai.lzy.channelmanager.channel.ChannelException;
import ai.lzy.channelmanager.channel.Endpoint;
import ai.lzy.channelmanager.graph.ChannelGraph;
import java.util.stream.Stream;

public interface ChannelController {
    Stream<Endpoint> executeBind(ChannelGraph channelGraph, Endpoint slot) throws ChannelException;

    void executeUnBind(ChannelGraph channelGraph, Endpoint slot) throws ChannelException;

    void executeDestroy(ChannelGraph channelGraph) throws ChannelException;
}
