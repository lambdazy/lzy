package ai.lzy.graph.db.impl;

import ai.lzy.graph.config.ServiceConfig;
import ai.lzy.model.db.StorageImpl;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;

@Singleton
@Requires(property = "graph-executor.database.enabled", value = "true")
public class GraphExecutorDataSource extends StorageImpl {
    public GraphExecutorDataSource(ServiceConfig config) {
        super(config.getDatabase(), "classpath:db/graph/migrations");
    }
}
