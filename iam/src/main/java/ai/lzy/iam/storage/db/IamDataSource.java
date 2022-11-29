package ai.lzy.iam.storage.db;

import ai.lzy.iam.configs.ServiceConfig;
import ai.lzy.model.db.StorageImpl;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;

@Singleton
@Requires(property = "iam.database.url")
@Requires(property = "iam.database.username")
@Requires(property = "iam.database.password")
public class IamDataSource extends StorageImpl {
    public IamDataSource(ServiceConfig config) {
        super(config.getDatabase(), "classpath:db/iam/migrations");
    }
}
