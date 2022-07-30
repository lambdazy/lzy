package ai.lzy.disk.configs;

import io.micronaut.context.annotation.ConfigurationProperties;
import javax.annotation.Nullable;

@ConfigurationProperties("disk-service.local-provider")
public record LocalStorageProviderConfig(
    @Nullable String disksLocation
) { }
