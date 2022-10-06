package ai.lzy.service.data.dao;

import ai.lzy.model.db.TransactionHandle;

import java.sql.SQLException;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

public interface ExecutionDao {
    default void saveSlots(String executionId, Set<String> slotsUri) throws SQLException {
        saveSlots(executionId, slotsUri, null);
    }

    void saveSlots(String executionId, Set<String> slotsUri, @Nullable TransactionHandle transaction)
        throws SQLException;

    default void saveChannels(Map<String, String> slot2channel) throws SQLException {
        saveChannels(slot2channel, null);
    }

    void saveChannels(Map<String, String> slot2channel, @Nullable TransactionHandle transaction) throws SQLException;

    Set<String> retainExistingSlots(Set<String> slotsUri) throws SQLException;

    Set<String> retainNonExistingSlots(String executionId, Set<String> slotsUri) throws SQLException;

    Map<String, String> findChannelsForOutputSlots(Set<String> slotsUri) throws SQLException;
}
