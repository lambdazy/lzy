package ai.lzy.channelmanager;

import ai.lzy.channelmanager.channel.Channel;
import ai.lzy.channelmanager.channel.ChannelException;
import ai.lzy.channelmanager.channel.Endpoint;
import ai.lzy.model.channel.ChannelSpec;
import java.util.stream.Stream;
import javax.annotation.Nullable;

public interface ChannelStorage {
    Channel create(ChannelSpec spec, String workflowId);

    @Nullable
    Channel get(String channelId);

    void destroy(String channelId) throws ChannelException;

    @Nullable
    Channel bound(Endpoint endpoint) throws ChannelException;

    Stream<Channel> channels();
}
