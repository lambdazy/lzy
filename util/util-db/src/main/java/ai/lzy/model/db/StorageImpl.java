package ai.lzy.model.db;

import com.google.common.annotations.VisibleForTesting;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import jakarta.annotation.PreDestroy;
import org.flywaydb.core.Flyway;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.function.Consumer;

public abstract class StorageImpl implements Storage {
    private static final String VALIDATION_QUERY_SQL = "select 1";

    private final ComboPooledDataSource dataSource;
    private volatile Consumer<Storage> onClose = null;

    protected StorageImpl(DatabaseConfiguration dbConfig, String migrationsPath) {
        dataSource = new ComboPooledDataSource();
        dataSource.setJdbcUrl(dbConfig.getUrl());
        dataSource.setUser(dbConfig.getUsername());
        dataSource.setPassword(dbConfig.getPassword());

        dataSource.setMinPoolSize(dbConfig.getMinPoolSize());
        dataSource.setMaxPoolSize(dbConfig.getMaxPoolSize());

        dataSource.setTestConnectionOnCheckout(true);
        dataSource.setPreferredTestQuery(VALIDATION_QUERY_SQL);

        var flyway = Flyway.configure()
            .dataSource(dbConfig.getUrl(), dbConfig.getUsername(), dbConfig.getPassword())
            .locations(migrationsPath)
            .load();
        flyway.migrate();
    }

    @Override
    public final Connection connect() throws SQLException {
        var conn = dataSource.getConnection();
        conn.setAutoCommit(true);
        conn.setTransactionIsolation(isolationLevel());
        return conn;
    }

    @PreDestroy
    public void close() {
        var fn = onClose;
        if (fn != null) {
            fn.accept(this);
        }
        dataSource.close();
    }

    @VisibleForTesting
    public void setOnClose(Consumer<Storage> onClose) {
        this.onClose = onClose;
    }

    protected int isolationLevel() {
        return Connection.TRANSACTION_REPEATABLE_READ;
    }
}
