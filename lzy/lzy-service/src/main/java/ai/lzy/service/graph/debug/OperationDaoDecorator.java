package ai.lzy.service.graph.debug;

import ai.lzy.longrunning.Operation;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.longrunning.dao.OperationDaoImpl;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.service.data.storage.LzyServiceStorage;
import io.micronaut.context.annotation.Requires;
import jakarta.annotation.Nullable;
import jakarta.inject.Named;
import jakarta.inject.Singleton;

import java.sql.SQLException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;


@Singleton
@Named("LzyServiceOperationDao")
@Requires(env = "test-mock")
public class OperationDaoDecorator implements OperationDao {
    private final Queue<Runnable> onCreateEvents = new ConcurrentLinkedQueue<>();
    private final AtomicInteger createCallsCounter = new AtomicInteger(0);

    private final OperationDao delegate;

    public OperationDaoDecorator(LzyServiceStorage storage) {
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

    @Nullable
    @Override
    public Operation update(String id, TransactionHandle transaction) throws SQLException {
        return delegate.update(id, transaction);
    }

    @Nullable
    @Override
    public Operation complete(String id, byte[] meta, byte[] response, TransactionHandle transaction)
        throws SQLException
    {
        return delegate.complete(id, meta, response, transaction);
    }

    @Nullable
    @Override
    public Operation complete(String id, byte[] response, TransactionHandle transaction) throws SQLException {
        return delegate.complete(id, response, transaction);
    }


    @Nullable
    @Override
    public Operation updateMeta(String id, byte[] meta, @Nullable TransactionHandle transaction) throws SQLException {
        return delegate.updateMeta(id, meta, transaction);
    }

    @Nullable
    @Override
    public Operation fail(String id, byte[] error, TransactionHandle transaction) throws SQLException {
        return delegate.fail(id, error, transaction);
    }

    @Override
    public boolean deleteCompletedOperation(String operationId, TransactionHandle transaction) throws SQLException {
        return delegate.deleteCompletedOperation(operationId, transaction);
    }

    @Override
    public int deleteOutdatedOperations(int hours) throws SQLException {
        return delegate.deleteOutdatedOperations(hours);
    }
}
