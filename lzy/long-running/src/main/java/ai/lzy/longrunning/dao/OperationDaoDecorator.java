package ai.lzy.longrunning.dao;

import ai.lzy.longrunning.Operation;
import ai.lzy.model.db.Storage;
import ai.lzy.model.db.TransactionHandle;
import com.google.protobuf.Any;
import com.google.rpc.Status;
import jakarta.annotation.Nullable;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;


public class OperationDaoDecorator implements OperationDao {
    private final Queue<Runnable> onCreateEvents = new ConcurrentLinkedQueue<>();
    private final AtomicInteger createCallsCounter = new AtomicInteger(0);

    private final OperationDao delegate;

    public OperationDaoDecorator(Storage storage) {
        this.delegate = new OperationDaoImpl(storage);
    }

    public void onCreate(Runnable action) {
        onCreateEvents.add(action);
    }

    public void onCreateCounter() {
        onCreate(createCallsCounter::incrementAndGet);
    }

    public int createCallsCount() {
        return createCallsCounter.get();
    }

    @Override
    public void create(Operation operation, @Nullable TransactionHandle transaction) throws SQLException {
        onCreateEvents.forEach(Runnable::run);
        delegate.create(operation, transaction);
    }

    @Nullable
    @Override
    public Operation getByIdempotencyKey(String idempotencyKey, @Nullable TransactionHandle transaction)
        throws SQLException
    {
        return delegate.getByIdempotencyKey(idempotencyKey, transaction);
    }

    @Nullable
    @Override
    public Operation get(String id, @Nullable TransactionHandle transaction) throws SQLException {
        return delegate.get(id, transaction);
    }

    @Override
    public void update(String id, TransactionHandle transaction) throws SQLException {
        delegate.update(id, transaction);
    }

    @Override
    public Operation complete(String id, @Nullable Any meta, Any response, TransactionHandle transaction)
        throws SQLException
    {
        return delegate.complete(id, meta, response, transaction);
    }

    @Override
    public Operation complete(String id, Any response, TransactionHandle transaction) throws SQLException {
        return delegate.complete(id, response, transaction);
    }


    @Override
    public Operation updateMeta(String id, @Nullable Any meta, @Nullable TransactionHandle transaction)
        throws SQLException
    {
        return delegate.updateMeta(id, meta, transaction);
    }

    @Override
    public Operation fail(String id, Status error, TransactionHandle transaction) throws SQLException {
        return delegate.fail(id, error, transaction);
    }

    @Override
    public void fail(Collection<String> ids, Status error, TransactionHandle transaction) throws SQLException {
        delegate.fail(ids, error, transaction);
    }

    @Override
    public boolean deleteCompletedOperation(String operationId, TransactionHandle transaction) throws SQLException {
        return delegate.deleteCompletedOperation(operationId, transaction);
    }

    @Override
    public int deleteOutdatedOperations(int hours) throws SQLException {
        return delegate.deleteOutdatedOperations(hours);
    }

    @Override
    public Operation failOperation(String operationId, Status error, TransactionHandle tx, Logger log)
        throws SQLException
    {
        return delegate.failOperation(operationId, error, tx, log);
    }
}
