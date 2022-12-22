package ai.lzy.longrunning.dao;

import ai.lzy.longrunning.Operation;
import ai.lzy.model.db.TransactionHandle;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;
import javax.annotation.Nullable;

import static ai.lzy.model.db.DbHelper.withRetries;

public interface OperationDao {

    String OPERATION_IDEMPOTENCY_KEY_CONSTRAINT = "idempotency_key_to_operation_index";

    void create(Operation operation, @Nullable TransactionHandle transaction) throws SQLException;

    @Nullable
    Operation getByIdempotencyKey(String idempotencyKey, @Nullable TransactionHandle transaction) throws SQLException;

    @Nullable
    Operation get(String id, @Nullable TransactionHandle transaction) throws SQLException;

    @Nullable
    Operation updateMetaAndResponse(String id, byte[] meta, byte[] response, @Nullable TransactionHandle transaction)
        throws SQLException;

    @Nullable
    Operation updateMeta(String id, byte[] meta, @Nullable TransactionHandle transaction) throws SQLException;

    @Nullable
    Operation updateResponse(String id, byte[] response, @Nullable TransactionHandle transaction) throws SQLException;

    @Nullable
    Operation updateError(String id, byte[] error, @Nullable TransactionHandle transaction) throws SQLException;

    @Nullable
    default Operation failOperation(String operationId, com.google.rpc.Status error,
                                    @Nullable TransactionHandle transaction, Logger log)
    {
        Operation op = null;

        log.info("Fail operation with error: { operationId: {}, errorCode: {}, message: {} }", operationId,
            io.grpc.Status.fromCodeValue(error.getCode()).getCode().name(), error.getMessage());

        try {
            op = withRetries(log, () -> updateError(operationId, error.toByteArray(), transaction));
        } catch (Exception ex) {
            log.error("Cannot fail operation {} with reason {}: {}",
                operationId, error.getMessage(), ex.getMessage(), ex);
        }

        if (op == null) {
            log.error("Cannot fail operation {} with reason {}: operation not found", operationId, error.getMessage());
        }

        return op;
    }

    @Nullable
    default Operation failOperation(String operationId, com.google.rpc.Status error, Logger log) {
        return failOperation(operationId, error, null, log);
    }
}
