package ai.lzy.service.data.dao;

import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Set;

public interface ExecutionDao {
    Set<String> getSlotsUriBy(String executionId) throws SQLException;

    void putSlotsUriFor(String executionId, Collection<String> slotsUri) throws SQLException;

    List<String> whichSlotsUriPresented(Collection<String> slotsUri) throws SQLException;
}
