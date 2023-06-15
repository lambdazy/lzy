package ai.lzy.longrunning.dao;

import ai.lzy.longrunning.Operation;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.NotFoundException;
import com.google.protobuf.Any;
import jakarta.annotation.Nullable;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;
import java.util.Collection;

public interface OperationDao {

    String OPERATION_IDEMPOTENCY_KEY_CONSTRAINT = "idempotency_key_to_operation_index";

    void create(Operation operation, @Nullable TransactionHandle transaction) throws SQLException;

    @Nullable
    Operation getByIdempotencyKey(String idempotencyKey, @Nullable TransactionHandle transaction) throws SQLException;

    @Nullable
    Operation get(String id, @Nullable TransactionHandle transaction) throws SQLException;

    /**
     * @throws ai.lzy.model.db.exceptions.NotFoundException if operation not exists
     * @throws OperationCompletedException                  if operation already completed
     * @throws SQLException                                 on any sql error
     */
    void update(String id, @Nullable TransactionHandle transaction) throws SQLException;

    /**
     * @return completed operation
     * @throws ai.lzy.model.db.exceptions.NotFoundException if operation not exists
     * @throws OperationCompletedException                  if operation already completed
     * @throws SQLException                                 on any sql error
     */
    Operation complete(String id, @Nullable Any meta, Any response, @Nullable TransactionHandle transaction)
        throws SQLException;

    /**
     * @return completed operation
     * @throws ai.lzy.model.db.exceptions.NotFoundException if operation not exists
     * @throws OperationCompletedException                  if operation already completed
     * @throws SQLException                                 on any sql error
     */
    Operation complete(String id, Any response, @Nullable TransactionHandle transaction) throws SQLException;

    /**
     * @return updated operation
     * @throws ai.lzy.model.db.exceptions.NotFoundException if operation not exists
     * @throws OperationCompletedException                  if operation already completed
     * @throws SQLException                                 on any sql error
     */
    Operation updateMeta(String id, Any meta, @Nullable TransactionHandle transaction) throws SQLException;

    /**
     * @return failed operation
     * @throws ai.lzy.model.db.exceptions.NotFoundException if operation not exists
     * @throws OperationCompletedException                  if operation already completed
     * @throws SQLException                                 on any sql error
     */
    Operation fail(String id, com.google.rpc.Status error, @Nullable TransactionHandle transaction) throws SQLException;

    /**
     * @throws SQLException on any sql error
     */
    void fail(Collection<String> ids, com.google.rpc.Status error, @Nullable TransactionHandle transaction)
        throws SQLException;

    boolean deleteCompletedOperation(String operationId, @Nullable TransactionHandle transaction) throws SQLException;

    int deleteOutdatedOperations(int hours) throws SQLException;


    default Operation failOperation(String operationId, com.google.rpc.Status error, @Nullable TransactionHandle tx,
                                    Logger log) throws SQLException
    {
        log.info("Trying to fail operation with error: { operationId: {}, errorCode: {}, message: {} }",
            operationId, io.grpc.Status.fromCodeValue(error.getCode()).getCode().name(), error.getMessage());

        try {
            return fail(operationId, error, tx);
        } catch (OperationCompletedException e) {
            log.error("Cannot fail operation {} with reason {}: operation already completed",
                operationId, error.getMessage());
            return get(operationId, null);
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
