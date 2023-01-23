package ai.lzy.scheduler.db;

import ai.lzy.model.db.StorageImpl;
import ai.lzy.scheduler.configs.ServiceConfig;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;

import java.sql.Connection;

@Singleton
@Requires(property = "scheduler.database.enabled", value = "true")
public class SchedulerDataSource extends StorageImpl {
    public SchedulerDataSource(ServiceConfig config) {
        super(config.getDatabase(), "classpath:db/scheduler/migrations");
    }

    @Override
    protected int isolationLevel() {
        return Connection.TRANSACTION_READ_COMMITTED;
    }
}
