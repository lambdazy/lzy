package ai.lzy.channelmanager.v2.db;

import ai.lzy.channelmanager.config.ChannelManagerConfig;
import ai.lzy.model.db.StorageImpl;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;

@Singleton
@Requires(property = "channel-manager.database.enabled", value = "true")
public class ChannelManagerDataSource extends StorageImpl {
    public ChannelManagerDataSource(ChannelManagerConfig config) {
        super(config.getDatabase(), "classpath:db/channel-manager/v2-migrations");
    }
}
