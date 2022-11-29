package ai.lzy.whiteboard.storage;

import ai.lzy.model.db.StorageImpl;
import ai.lzy.whiteboard.AppConfig;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;

@Singleton
@Requires(property = "whiteboard.database.url")
@Requires(property = "whiteboard.database.username")
@Requires(property = "whiteboard.database.password")
public class WhiteboardDataSource extends StorageImpl {
    public WhiteboardDataSource(AppConfig config) {
        super(config.getDatabase(), "classpath:db/whiteboard/migrations");
    }
}
