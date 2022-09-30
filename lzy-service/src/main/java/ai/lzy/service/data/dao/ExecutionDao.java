package ai.lzy.service.data.dao;

import java.sql.SQLException;
import java.util.Set;

public interface ExecutionDao {
    void saveSlots(String executionId, Set<String> slotsUri) throws SQLException;

    Set<String> retainExistingSlots(Set<String> slotsUri) throws SQLException;

    Set<String> retainNonExistingSlots(String executionId, Set<String> slotsUri) throws SQLException;
}
