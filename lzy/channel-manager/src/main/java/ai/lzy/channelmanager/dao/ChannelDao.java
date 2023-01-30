package ai.lzy.channelmanager.dao;

import ai.lzy.channelmanager.model.Connection;
import ai.lzy.channelmanager.model.Endpoint;
import ai.lzy.channelmanager.model.channel.Channel;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.slot.SlotInstance;

import java.sql.SQLException;
import java.util.List;
import javax.annotation.Nullable;

public interface ChannelDao {

    void insertChannel(String channelId, String executionId, String workflowName, String userId,
                       Channel.Spec channelSpec, @Nullable TransactionHandle transaction) throws SQLException;
    void markChannelDestroying(String channelId, @Nullable TransactionHandle transaction) throws SQLException;
    void removeChannel(String channelId, @Nullable TransactionHandle transaction) throws SQLException;

    void insertBindingEndpoint(SlotInstance slot, Endpoint.SlotOwner owner,
                               @Nullable TransactionHandle transaction) throws SQLException;
    void markEndpointBound(String endpointUri, @Nullable TransactionHandle transaction) throws SQLException;
    void markEndpointUnbinding(String endpointUri, @Nullable TransactionHandle transaction) throws SQLException;
    void markAllEndpointsUnbinding(String channelId, @Nullable TransactionHandle transaction) throws SQLException;
    void removeEndpoint(String endpointUri, @Nullable TransactionHandle transaction) throws SQLException;

    void insertConnection(String channelId, Connection connection,
                          @Nullable TransactionHandle transaction) throws SQLException;
    void markConnectionAlive(String channelId, String senderUri, String receiverUri,
                             @Nullable TransactionHandle transaction) throws SQLException;
    void markConnectionDisconnecting(String channelId, String senderUri, String receiverUri,
                                     @Nullable TransactionHandle transaction) throws SQLException;
    void removeConnection(String channelId, String senderUri, String receiverUri,
                          @Nullable TransactionHandle transaction) throws SQLException;

    @Nullable
    Channel findChannel(String channelId, @Nullable TransactionHandle transaction) throws SQLException;
    @Nullable
    Channel findChannel(String channelId, Channel.LifeStatus lifeStatus,
                        @Nullable TransactionHandle transaction) throws SQLException;


    List<Channel> listChannels(String executionId) throws SQLException;
    List<Channel> listChannels(String executionId, Channel.LifeStatus lifeStatus) throws SQLException;

    @Nullable
    Endpoint findEndpoint(String endpointUri, @Nullable TransactionHandle transaction) throws SQLException;
}
