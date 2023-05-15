package ai.lzy.service.dao.impl;

import ai.lzy.model.db.StorageImpl;
import ai.lzy.service.config.LzyServiceConfig;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;

@Singleton
@Requires(property = "lzy-service.database.url")
public class LzyServiceStorage extends StorageImpl {
    public LzyServiceStorage(LzyServiceConfig configuration) {
        super(configuration.getDatabase(), "classpath:db/lzy-service/migrations");
    }
}
