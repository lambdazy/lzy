package ai.lzy.service.data.dao;

import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface ExecutionDao {
    Set<String> getSlotsUriStoredOnPortalBy(String executionId) throws SQLException;

    void indicateSlotsUriStoredOnPortal(String executionId, Collection<String> slotsUri) throws SQLException;

    List<String> whichSlotsUriPresented(Collection<String> slotsUri) throws SQLException;

    /**
     * Returns channel IDs for slot URIs.
     *
     * @param slotsUri collection of slot URIs which channels requested
     * @return SlotData for each slot URI if its channel already exists
     */
    Map<String, SlotData> getExecutionAndChannelIdBy(Collection<String> slotsUri) throws SQLException;

    /**
     * Update channel IDs for specified slot URIs.
     * @param executionId ID of execution in which specified slots exist
     * @param slot2channel channel IDs for slot URIs
     */
    void addChannelIdsFor(String executionId, Map<String, String> slot2channel) throws SQLException;

    record SlotData(String channelId, String executionId) {}
}
