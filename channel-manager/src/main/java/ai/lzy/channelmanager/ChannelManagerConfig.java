package ai.lzy.channelmanager;

import ai.lzy.iam.config.IamClientConfiguration;
import ai.lzy.model.db.DatabaseConfiguration;
import io.micronaut.context.annotation.ConfigurationBuilder;
import io.micronaut.context.annotation.ConfigurationProperties;
import io.micronaut.core.bind.annotation.Bindable;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@ConfigurationProperties("channel-manager")
public final class ChannelManagerConfig {
    private String address;
    private String whiteboardAddress;

    @ConfigurationBuilder("iam")
    private final IamClientConfiguration iam = new IamClientConfiguration();

    @ConfigurationBuilder("database")
    private final DatabaseConfiguration database = new DatabaseConfiguration();
}
