package ai.lzy.longrunning.dao;

import ai.lzy.longrunning.Operation;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.NotFoundException;
import jakarta.annotation.Nullable;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;

public interface OperationDao {

    String OPERATION_IDEMPOTENCY_KEY_CONSTRAINT = "idempotency_key_to_operation_index";

    void create(Operation operation, @Nullable TransactionHandle transaction) throws SQLException;

    @Nullable
    Operation getByIdempotencyKey(String idempotencyKey, @Nullable TransactionHandle transaction) throws SQLException;

    @Nullable
    Operation get(String id, @Nullable TransactionHandle transaction) throws SQLException;

    /**
     * @return <code>null</code> on success, actual (completed) operation on fail
     * @throws ai.lzy.model.db.exceptions.NotFoundException if operation not exists
     * @throws SQLException on any sql error
     */
    @Nullable
    Operation update(String id, @Nullable TransactionHandle transaction) throws SQLException;

    /**
     * @return <code>null</code> on success, actual (completed) operation on fail
     * @throws ai.lzy.model.db.exceptions.NotFoundException if operation not exists
     * @throws SQLException on any sql error
     */
    @Nullable
    Operation complete(String id, byte[] meta, byte[] response, @Nullable TransactionHandle transaction)
        throws SQLException;

    /**
     * @return <code>null</code> on success, actual (completed) operation on fail
     * @throws ai.lzy.model.db.exceptions.NotFoundException if operation not exists
     * @throws SQLException on any sql error
     */
    @Nullable
    Operation complete(String id, byte[] response, @Nullable TransactionHandle transaction) throws SQLException;

    /**
     * @return <code>null</code> on success, actual (completed) operation on fail
     * @throws ai.lzy.model.db.exceptions.NotFoundException if operation not exists
     * @throws SQLException on any sql error
     */
    @Nullable
    Operation updateMeta(String id, byte[] meta, @Nullable TransactionHandle transaction) throws SQLException;

    /**
     * @return <code>null</code> on success, actual (completed) operation on fail
     * @throws ai.lzy.model.db.exceptions.NotFoundException if operation not exists
     * @throws SQLException on any sql error
     */
    @Nullable
    Operation fail(String id, byte[] error, @Nullable TransactionHandle transaction) throws SQLException;

    /**
     * @return <code>null</code> on success, actual (completed) operation on fail
     * @throws ai.lzy.model.db.exceptions.NotFoundException if operation not exists
     * @throws SQLException on any sql error
     */
    @Nullable
    default Operation fail(String id, com.google.rpc.Status error, @Nullable TransactionHandle transaction)
        throws SQLException
    {
        return fail(id, error.toByteArray(), transaction);
    }

    boolean deleteCompletedOperation(String operationId, @Nullable TransactionHandle transaction) throws SQLException;

    int deleteOutdatedOperations(int hours) throws SQLException;


    /**
     * @return failed operation (!!!)
     */
    @Nullable
    default Operation failOperation(String operationId, com.google.rpc.Status error, @Nullable TransactionHandle tx,
                                    Logger log) throws SQLException
    {
        log.info("Trying to fail operation with error: { operationId: {}, errorCode: {}, message: {} }",
            operationId, io.grpc.Status.fromCodeValue(error.getCode()).getCode().name(), error.getMessage());

        try {
            var op = fail(operationId, error.toByteArray(), tx);
            if (op != null) {
                log.error("Cannot fail operation {} with reason {}: operation already completed",
                    operationId, error.getMessage());
                return op;
            }
            return get(operationId, tx);
        } catch (NotFoundException e) {
            log.error("Cannot fail operation {} with reason {}: operation not found", operationId, error.getMessage());
            return null;
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            log.error("Cannot fail operation {} with reason {}: {}",
                operationId, error.getMessage(), e.getMessage(), e);
            throw new SQLException(e);
        }
    }
}
