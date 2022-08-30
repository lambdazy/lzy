package ai.lzy.model.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;
import javax.annotation.Nullable;

public class DelegatingTransactionHandle implements TransactionHandle {

    private final TransactionHandle transaction;
    private final boolean acquired;

    DelegatingTransactionHandle(TransactionHandle transaction) {
        this.transaction = transaction;
        acquired = true;
    }

    DelegatingTransactionHandle(Storage storage, @Nullable TransactionHandle transaction) {
        if (transaction == null) {
            this.transaction = new TransactionHandleImpl(storage);
            acquired = false;
        } else {
            this.transaction = Objects.requireNonNull(transaction);
            acquired = true;
        }
    }

    public synchronized Connection connect() throws SQLException {
        return transaction.connect();
    }

    public synchronized void commit() throws SQLException {
        transaction.connect();
    }

    @Override
    public synchronized void close() throws SQLException {
        if (!acquired) {
            transaction.close();
        }
    }
}
