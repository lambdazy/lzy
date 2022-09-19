package ai.lzy.channelmanager.db;

import ai.lzy.channelmanager.channel.Channel;
import ai.lzy.channelmanager.channel.ChannelSpec;
import ai.lzy.channelmanager.channel.Endpoint;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.v1.channel.LCM;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

public interface ChannelStorage {

    void insertChannel(String channelId, String userId, String workflowId,
                       String channelName, LCM.ChannelSpec.TypeCase channelType, ChannelSpec channelSpec,
                       @Nullable TransactionHandle transaction) throws SQLException;

    void removeChannel(String channelId, @Nullable TransactionHandle transaction) throws SQLException;

    void insertEndpoint(Endpoint endpoint, TransactionHandle transaction) throws SQLException;

    void insertEndpointConnections(String channelId, Map<Endpoint, Endpoint> edges,
                                   TransactionHandle transaction) throws SQLException;

    void removeEndpointWithConnections(Endpoint endpoint, TransactionHandle transaction) throws SQLException;

    void setChannelLifeStatus(String channelId, ChannelLifeStatus lifeStatus,
                              @Nullable TransactionHandle transaction) throws SQLException;
    void setChannelLifeStatus(String userId, String workflowId, ChannelLifeStatus lifeStatus,
                              @Nullable TransactionHandle transaction) throws SQLException;

    @Nullable
    Channel findChannel(String channelId, ChannelLifeStatus lifeStatus,
                        @Nullable TransactionHandle transaction) throws SQLException;

    List<Channel> listChannels(String userId, String workflowId, ChannelLifeStatus lifeStatus,
                               @Nullable TransactionHandle transaction) throws SQLException;

    enum ChannelLifeStatus {
        ALIVE,
        DESTROYING,
        ;
    }
}
