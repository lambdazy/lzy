package ai.lzy.disk.configs;

import io.micronaut.context.annotation.ConfigurationProperties;

import jakarta.annotation.Nullable;
import javax.annotation.Nullable;

@ConfigurationProperties("disk-service.amazonS3")
public record S3Config(
    @Nullable String accessKey,
    @Nullable String secretKey,
    @Nullable String endpoint,
    @Nullable String region
) { }
