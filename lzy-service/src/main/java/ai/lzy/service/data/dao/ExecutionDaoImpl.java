package ai.lzy.service.data.dao;

import ai.lzy.model.db.Storage;
import ai.lzy.service.data.storage.LzyServiceStorage;
import jakarta.inject.Singleton;

import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Singleton
public class ExecutionDaoImpl implements ExecutionDao {
    private final Storage storage;

    public ExecutionDaoImpl(LzyServiceStorage storage) {
        this.storage = storage;
    }

    @Override
    public Set<String> getSlotsUriStoredOnPortalBy(String executionId) throws SQLException {
        throw new UnsupportedOperationException("not implemented yet");
    }

    @Override
    public void indicateSlotsUriStoredOnPortal(String executionId, Collection<String> slotsUri) throws SQLException {
        throw new UnsupportedOperationException("not implemented yet");
    }

    @Override
    public List<String> whichSlotsUriPresented(Collection<String> slotsUri) throws SQLException {
        throw new UnsupportedOperationException("not implemented yet");
    }

    @Override
    public Map<String, SlotData> getExecutionAndChannelIdBy(Collection<String> slotsUri) {
        throw new UnsupportedOperationException("not implemented yet");
    }

    @Override
    public void addChannelIdsFor(String executionId, Map<String, String> slot2channel) throws SQLException {
        throw new UnsupportedOperationException("not implemented yet");
    }
}
