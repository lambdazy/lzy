package ai.lzy.channelmanager.v2.db;

import ai.lzy.channelmanager.v2.model.Channel;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.v1.common.LC;
import ai.lzy.v1.common.LMD;
import jakarta.annotation.Nullable;

import java.sql.SQLException;
import java.util.List;

public interface ChannelDao {
    Channel create(String id, String userId, String executionId, String workflowName,
                   @Nullable LMD.DataScheme dataScheme, @Nullable String storageProducerUri,
                   @Nullable String storageConsumerUri, @Nullable TransactionHandle tx) throws SQLException;

    @Nullable
    Channel drop(String channelId, @Nullable TransactionHandle tx) throws SQLException;

    void dropAll(String executionId, @Nullable TransactionHandle tx) throws SQLException;

    @Nullable
    Channel find(String userId, String executionId, @Nullable String storageProducerUri,
                 @Nullable String storageConsumerUri, @Nullable TransactionHandle tx) throws SQLException;

    @Nullable
    Channel get(String channelId, @Nullable TransactionHandle tx) throws SQLException;

    List<ChannelStatus> list(String executionId, @Nullable List<String> channelIdsFilter,
                             @Nullable TransactionHandle tx) throws SQLException;

    record ChannelStatus(
        Channel channel,
        List<LC.PeerDescription> consumers,
        List<LC.PeerDescription> producers
    ) {}
}
