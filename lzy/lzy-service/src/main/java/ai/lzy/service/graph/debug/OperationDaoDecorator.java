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
@Requires(beans = LzyServiceStorage.class, env = "test-mock")
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
    public Operation updateMetaAndResponse(String id, byte[] meta, byte[] response,
                                           @Nullable TransactionHandle transaction) throws SQLException
    {
        return delegate.updateMetaAndResponse(id, meta, response, transaction);
    }

    @Nullable
    @Override
    public Operation updateMeta(String id, byte[] meta, @Nullable TransactionHandle transaction) throws SQLException {
        return delegate.updateMeta(id, meta, transaction);
    }

    @Nullable
    @Override
    public Operation updateResponse(String id, byte[] response, @Nullable TransactionHandle transaction)
        throws SQLException
    {
        return delegate.updateResponse(id, response, transaction);
    }

    @Nullable
    @Override
    public Operation updateError(String id, byte[] error, @Nullable TransactionHandle transaction) throws SQLException {
        return delegate.updateError(id, error, transaction);
    }
}
