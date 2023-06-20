package ai.lzy.service.dao;

import ai.lzy.model.db.TransactionHandle;
import jakarta.annotation.Nullable;

import java.sql.SQLException;
import java.util.Collection;
import java.util.List;

public interface ExecutionOperationsDao {
    void createStartOp(String opId, String instanceId, String execId, @Nullable TransactionHandle transaction)
        throws SQLException;

    void createFinishOp(String opId, String instanceId, String execId, @Nullable TransactionHandle transaction)
        throws SQLException;

    void createAbortOp(String opId, String instanceId, String execId, @Nullable TransactionHandle transaction)
        throws SQLException;

    void createExecGraphOp(String opId, String instanceId, String execId, ExecuteGraphState initial,
                           @Nullable TransactionHandle transaction) throws SQLException;

    void deleteOp(String opId, @Nullable TransactionHandle transaction) throws SQLException;

    void deleteOps(Collection<String> opIds, @Nullable TransactionHandle transaction) throws SQLException;

    void putState(String opId, ExecuteGraphState state, @Nullable TransactionHandle transaction) throws SQLException;

    ExecuteGraphState getState(String opId, @Nullable TransactionHandle transaction) throws SQLException;

    List<OpInfo> listOpsInfo(String execId, @Nullable TransactionHandle transaction) throws SQLException;

    List<String> listOpsIdsToCancel(String execId, @Nullable TransactionHandle transaction) throws SQLException;

    List<ExecutionOpState> listUncompletedOps(String instanceId, @Nullable TransactionHandle transaction)
        throws SQLException;

    record OpInfo(String opId, OpType type) {}

    record ExecutionOpState(
        OpType type,
        String opId,
        String opDesc,
        String idempotencyKey,
        String userId,
        String wfName,
        String execId
    ) {}

    enum OpType {
        START_EXECUTION,
        FINISH_EXECUTION,
        ABORT_EXECUTION,
        EXECUTE_GRAPH;

        public static boolean isStop(OpType type) {
            return type == FINISH_EXECUTION || type == ABORT_EXECUTION;
        }
    }
}
