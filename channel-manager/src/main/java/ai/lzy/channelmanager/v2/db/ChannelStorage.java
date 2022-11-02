package ai.lzy.channelmanager.v2.db;

import ai.lzy.channelmanager.channel.ChannelSpec;
import ai.lzy.channelmanager.channel.Endpoint;
import ai.lzy.channelmanager.channel.v2.Channel;
import ai.lzy.model.db.TransactionHandle;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

public interface ChannelStorage {

    void insertChannel(String channelId, String executionId, ChannelSpec channelSpec,
                       @Nullable TransactionHandle transaction) throws SQLException;

    void removeChannel(String channelId, @Nullable TransactionHandle transaction) throws SQLException;

    void insertEndpoint(Endpoint endpoint, TransactionHandle transaction) throws SQLException;

    void insertEndpointConnections(String channelId, Map<Endpoint, Endpoint> edges,
                                   TransactionHandle transaction) throws SQLException;

    void removeEndpointWithConnections(Endpoint endpoint, TransactionHandle transaction) throws SQLException;

    void setChannelLifeStatusDestroying(
        String channelId, @Nullable TransactionHandle transaction) throws SQLException;
    void setChannelLifeStatusDestroyingOfExecution(
        String executionId, @Nullable TransactionHandle transaction) throws SQLException;

    @Nullable
    Channel findChannel(String channelId, ChannelLifeStatus lifeStatus,
                        @Nullable TransactionHandle transaction) throws SQLException;

    List<Channel> listChannels(String executionId, ChannelLifeStatus lifeStatus,
                               @Nullable TransactionHandle transaction) throws SQLException;

    enum ChannelLifeStatus {
        ALIVE,
        DESTROYING,
        ;
    }
}
