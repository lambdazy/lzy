package ai.lzy.jobsutils.db;

import ai.lzy.jobsutils.configs.ServiceConfig;
import ai.lzy.model.db.StorageImpl;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;

@Singleton
@Requires(property = "jobs.database.enabled", value = "true")
public class JobsDataSource extends StorageImpl {

    public JobsDataSource(ServiceConfig config) {
        super(config.getDatabase(), "classpath:db/jobs/migrations", "jobs_migrations_history", "jobs");
    }
}
