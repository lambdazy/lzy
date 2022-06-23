package ai.lzy.server.channel;

public interface ChannelController {
    void executeBind(ChannelGraph channelGraph, Endpoint slot) throws ChannelException;

    void executeUnBind(ChannelGraph channelGraph, Endpoint slot) throws ChannelException;

    void executeDestroy(ChannelGraph channelGraph) throws ChannelException;
}
