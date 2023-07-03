package ai.lzy.service.dao;

import ai.lzy.model.db.TransactionHandle;
import jakarta.annotation.Nullable;

import java.sql.SQLException;
import java.util.List;

public interface GraphDao {
    void put(GraphDescription description, @Nullable TransactionHandle transaction) throws SQLException;

    List<GraphDescription> getAll(String execId) throws SQLException;

    record GraphDescription(
        String graphId,
        String executionId
    )
    {
        public String toJson() {
            return "{ graphId: " + graphId + ", execId: " + executionId + " }";
        }
    }
}
