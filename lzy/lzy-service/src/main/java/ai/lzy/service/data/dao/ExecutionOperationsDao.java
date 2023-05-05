package ai.lzy.service.data.dao;

import ai.lzy.model.db.TransactionHandle;
import ai.lzy.service.data.ExecuteGraphOperationState;
import ai.lzy.service.data.StartExecutionOperationState;
import ai.lzy.service.data.StopExecutionOperationState;
import jakarta.annotation.Nullable;

import java.sql.SQLException;
import java.util.List;

public interface ExecutionOperationsDao {
    void create(String opId, String instanceId, String execId,
                @Nullable StartExecutionOperationState initial,
                @Nullable TransactionHandle transaction) throws SQLException;

    void create(String opId, String instanceId, String execId,
                @Nullable StopExecutionOperationState initial,
                @Nullable TransactionHandle transaction) throws SQLException;

    void create(String opId, String instanceId, String execId,
                @Nullable ExecuteGraphOperationState initial,
                @Nullable TransactionHandle transaction) throws SQLException;

    List<OpInfo> get(String execId, @Nullable TransactionHandle transaction) throws SQLException;

    record OpInfo(String opId, OpType type) {}

    enum OpType {
        START, STOP, EXECUTE_GRAPH
    }
}
