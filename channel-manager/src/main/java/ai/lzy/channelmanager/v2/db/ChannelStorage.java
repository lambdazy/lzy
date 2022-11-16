package ai.lzy.channelmanager.v2.db;

import ai.lzy.channelmanager.channel.ChannelSpec;
import ai.lzy.channelmanager.v2.model.Channel;
import ai.lzy.channelmanager.v2.model.Connection;
import ai.lzy.channelmanager.v2.model.Endpoint;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.NotFoundException;

import java.sql.SQLException;
import java.util.List;
import javax.annotation.Nullable;

public interface ChannelStorage {

    void insertChannel(String channelId, String executionId, ChannelSpec channelSpec,
                       @Nullable TransactionHandle transaction) throws SQLException;
    void markChannelDestroying(String channelId, @Nullable TransactionHandle transaction) throws SQLException;
    void removeChannel(String channelId, @Nullable TransactionHandle transaction) throws SQLException;

    void insertBindingEndpoint(Endpoint endpoint, @Nullable TransactionHandle transaction) throws SQLException;
    void markEndpointBound(String endpointUri, @Nullable TransactionHandle transaction) throws SQLException;
    void markEndpointUnbinding(String endpointUri, @Nullable TransactionHandle transaction) throws SQLException;
    void markAllEndpointsUnbinding(String channelId, @Nullable TransactionHandle transaction) throws SQLException;
    void removeEndpointWithoutConnections(String endpointUri,
                                          @Nullable TransactionHandle transaction) throws SQLException;

    void insertConnection(String channelId, Connection connection,
                          @Nullable TransactionHandle transaction) throws SQLException;
    void markConnectionAlive(String channelId, String senderUri, String receiverUri,
                             @Nullable TransactionHandle transaction) throws SQLException;
    void markConnectionDisconnecting(String channelId, String senderUri, String receiverUri,
                                     @Nullable TransactionHandle transaction) throws SQLException;
    void removeEndpointConnection(String channelId, String senderUri, String receiverUri,
                                  @Nullable TransactionHandle transaction) throws SQLException;

    @Nullable
    Channel findChannel(String channelId, @Nullable TransactionHandle transaction) throws SQLException;
    @Nullable
    Channel findChannel(String channelId, Channel.LifeStatus lifeStatus,
                        @Nullable TransactionHandle transaction) throws SQLException;


    List<Channel> listChannels(String executionId, Channel.LifeStatus lifeStatus,
                               @Nullable TransactionHandle transaction) throws SQLException;

    @Nullable
    Endpoint findEndpoint(String endpointUri, @Nullable TransactionHandle transaction) throws SQLException;

    default Endpoint getEndpoint(String endpointUri, @Nullable TransactionHandle transaction) throws SQLException
    {
        Endpoint endpoint = findEndpoint(endpointUri, transaction);
        if (endpoint == null) {
            throw new NotFoundException(String.format(
                "Endpoint %s with lifeStatus %s not found", endpointUri));
        }
        return endpoint;
    }

}
