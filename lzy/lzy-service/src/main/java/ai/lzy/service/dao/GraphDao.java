package ai.lzy.service.dao;

import ai.lzy.model.db.TransactionHandle;
import jakarta.annotation.Nullable;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

public interface GraphDao {
    void put(GraphDescription description, @Nullable TransactionHandle transaction) throws SQLException;

    @Nullable
    GraphDescription get(String graphId, String execId) throws SQLException;

    List<GraphDescription> getAll(String execId) throws SQLException;

    record GraphDescription(
        String graphId,
        String executionId,
        List<String> portalInputSlotNames
    )
    {
        public String toJson() {
            return "{ graphId: " + graphId + ", execId: " + executionId + ", portalInputs: "
                + portalInputSlotNames.stream().collect(Collectors.joining(", ", "{ ", " }")) + " }";
        }
    }
}
