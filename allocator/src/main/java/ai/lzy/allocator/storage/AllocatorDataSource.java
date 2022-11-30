package ai.lzy.allocator.storage;

import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.model.db.StorageImpl;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;

@Singleton
@Requires(property = "allocator.database.enabled", value = "true")
public class AllocatorDataSource extends StorageImpl {
    public AllocatorDataSource(ServiceConfig config) {
        super(config.getDatabase(), "classpath:db/allocator/migrations");
    }
}
