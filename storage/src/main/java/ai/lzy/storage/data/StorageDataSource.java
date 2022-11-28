package ai.lzy.storage.data;

import ai.lzy.model.db.StorageImpl;
import ai.lzy.storage.config.StorageConfig;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;

@Singleton
@Requires(property = "storage.database.enabled", value = "true")
public class StorageDataSource extends StorageImpl {
    public StorageDataSource(StorageConfig config) {
        super(config.getDatabase(), "classpath:db/storage/migrations");
    }
}
