package ai.lzy.allocator.disk;

import ai.lzy.util.auth.YcCredentials;
import io.micronaut.context.annotation.ConfigurationProperties;
import io.micronaut.core.bind.annotation.Bindable;
import javax.annotation.Nullable;

@ConfigurationProperties("disk-manager")
public class DiskManagerConfig {
    public record ServiceConfig(
        @Bindable(defaultValue = "10") int defaultOperationTimeoutMin
    ) {}

    @ConfigurationProperties("credentials")
    public record Credentials(
        @Nullable YcCredentials credentials
    ) {}

    @ConfigurationProperties("database")
    public record DbConfig(
        String url,
        String username,
        String password,
        @Bindable(defaultValue = "5")  int minPoolSize,
        @Bindable(defaultValue = "10") int maxPoolSize
    ) {}
}
