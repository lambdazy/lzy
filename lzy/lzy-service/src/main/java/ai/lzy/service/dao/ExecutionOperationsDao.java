package ai.lzy.service.dao;

import ai.lzy.model.db.TransactionHandle;
import jakarta.annotation.Nullable;

import java.sql.SQLException;
import java.util.List;

public interface ExecutionOperationsDao {
    void createStartOp(String opId, String instanceId, String execId, @Nullable TransactionHandle transaction)
        throws SQLException;

    void createStopOp(String opId, String instanceId, String execId, @Nullable TransactionHandle transaction)
        throws SQLException;

    void createExecGraphOp(String opId, String instanceId, String execId, @Nullable TransactionHandle transaction)
        throws SQLException;

    void putState(String opId, ExecuteGraphState state, @Nullable TransactionHandle transaction) throws SQLException;

    List<OpInfo> listOpsInfo(String execId, @Nullable TransactionHandle transaction) throws SQLException;

    List<String> listOpsIdsToCancel(String execId, @Nullable TransactionHandle transaction) throws SQLException;

    record OpInfo(String opId, OpType type) {}

    enum OpType {
        START_EXECUTION, STOP_EXECUTION, EXECUTE_GRAPH
    }
}
