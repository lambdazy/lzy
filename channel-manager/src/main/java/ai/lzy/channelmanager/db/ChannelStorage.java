package ai.lzy.channelmanager.db;

import ai.lzy.channelmanager.channel.Channel;
import ai.lzy.channelmanager.channel.Endpoint;
import ai.lzy.model.channel.ChannelSpec;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.v1.Channels;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

public interface ChannelStorage {

    void insertChannel(String channelId, String userId, String workflowId,
                       String channelName, Channels.ChannelSpec.TypeCase channelType, ChannelSpec channelSpec,
                       @Nullable TransactionHandle transaction);

    void removeChannel(String channelId, @Nullable TransactionHandle transaction);

    void lockChannel(String channelId, @Nullable TransactionHandle transaction);

    void insertEndpoint(String channelId, Endpoint endpoint, TransactionHandle transaction);

    void insertEndpointConnections(String channelId, Map<Endpoint, Endpoint> edges, TransactionHandle transaction);

    void removeEndpointWithConnections(String channelId, Endpoint endpoint, TransactionHandle transaction);

    void setChannelLifeStatus(String channelId, ChannelLifeStatus lifeStatus,
                              @Nullable TransactionHandle transaction);
    void setChannelLifeStatus(String userId, String workflowId, ChannelLifeStatus lifeStatus,
                              @Nullable TransactionHandle transaction);

    @Nullable
    Channel findChannel(String channelId, ChannelLifeStatus lifeStatus,
                        @Nullable TransactionHandle transaction);

    List<Channel> listChannels(String userId, String workflowId, ChannelLifeStatus lifeStatus,
                               @Nullable TransactionHandle transaction);

    List<String> listBoundChannels(String userId, String workflowId, String slotUri,
                                   @Nullable TransactionHandle transaction);

    enum ChannelLifeStatus {
        ALIVE,
        DESTROYING,
        ;
    }
}
