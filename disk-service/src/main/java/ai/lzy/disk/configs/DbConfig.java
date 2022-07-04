package ai.lzy.disk.configs;

import io.micronaut.context.annotation.ConfigurationProperties;
import io.micronaut.context.annotation.Requires;
import io.micronaut.core.bind.annotation.Bindable;
import javax.annotation.Nullable;

@ConfigurationProperties("disk-service.database")
public record DbConfig(
    @Nullable String url,
    @Nullable String username,
    @Nullable String password,
    @Bindable(defaultValue = "1") int minPoolSize,
    @Bindable(defaultValue = "5") int maxPoolSize
) { }
