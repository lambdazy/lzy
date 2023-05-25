package ai.lzy.graph.db.impl;

import java.sql.Connection;

import ai.lzy.graph.config.ServiceConfig;
import ai.lzy.model.db.StorageImpl;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;

@Singleton
@Requires(property = "graph-executor-2.database.enabled", value = "true")
public class GraphExecutorDataSource extends StorageImpl {
    public GraphExecutorDataSource(ServiceConfig config) {
        super(config.getDatabase(), "classpath:db/graph/migrations");
    }

    @Override
    protected int isolationLevel() {
        return Connection.TRANSACTION_READ_COMMITTED;
    }
}