package ai.lzy.model.db;

import jakarta.annotation.Nullable;

import java.sql.Connection;
import java.sql.SQLException;

public interface TransactionHandle extends AutoCloseable {

    Connection connect() throws SQLException;

    void commit() throws SQLException;

    @Override
    void close() throws SQLException;

    static TransactionHandle create(Storage storage) {
        return new TransactionHandleImpl(storage);
    }

    static TransactionHandle getOrCreate(Storage storage, @Nullable TransactionHandle transaction) {
        return new DelegatingTransactionHandle(storage, transaction);
    }

    static TransactionHandle delegate(TransactionHandle transaction) {
        return new DelegatingTransactionHandle(transaction);
    }

}
