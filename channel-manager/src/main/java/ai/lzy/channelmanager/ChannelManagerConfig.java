package ai.lzy.channelmanager;

import ai.lzy.iam.config.IamClientConfiguration;
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

    @ConfigurationProperties("database")
    public record DbConfig(
        @Nullable String url,
        @Nullable String username,
        @Nullable String password,
        @Bindable(defaultValue = "5") int minPoolSize,
        @Bindable(defaultValue = "10") int maxPoolSize
    ) { }
}
