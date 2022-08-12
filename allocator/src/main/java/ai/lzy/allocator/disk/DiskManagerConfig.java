package ai.lzy.allocator.disk;

import io.micronaut.context.annotation.ConfigurationProperties;
import io.micronaut.core.bind.annotation.Bindable;

@ConfigurationProperties("disk-manager")
public class DiskManagerConfig {
    @ConfigurationProperties("database")
    public record DbConfig(
        String url,
        String username,
        String password,
        @Bindable(defaultValue = "5")  int minPoolSize,
        @Bindable(defaultValue = "10") int maxPoolSize
    ) {}
}
