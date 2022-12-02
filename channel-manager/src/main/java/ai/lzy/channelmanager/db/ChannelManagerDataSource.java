package ai.lzy.channelmanager.db;

import ai.lzy.channelmanager.v2.config.ChannelManagerConfig;
import ai.lzy.model.db.StorageImpl;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;

@Singleton
@Requires(property = "channel-manager.database.url")
@Requires(property = "channel-manager.database.username")
@Requires(property = "channel-manager.database.password")
public class ChannelManagerDataSource extends StorageImpl {
    public ChannelManagerDataSource(ChannelManagerConfig config) {
        super(config.getDatabase(), "classpath:db/channel-manager/migrations");
    }
}
