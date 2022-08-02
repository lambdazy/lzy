package ai.lzy.channelmanager;

import io.micronaut.context.annotation.ConfigurationProperties;
import javax.annotation.Nullable;
import io.micronaut.core.bind.annotation.Bindable;
import javax.annotation.Nullable;

@ConfigurationProperties("channel-manager")
public record ChannelManagerConfig(
    @Nullable String address,
    @Nullable String whiteboardAddress,
    @Nullable Iam iam
) {
    @ConfigurationProperties("iam")
    public record Iam(
        @Nullable String address
    ) { }

    @ConfigurationProperties("database")
    public record DbConfig(
        @Nullable String url,
        @Nullable String username,
        @Nullable String password,
        @Bindable(defaultValue = "5") int minPoolSize,
        @Bindable(defaultValue = "10") int maxPoolSize
    ) { }
}
