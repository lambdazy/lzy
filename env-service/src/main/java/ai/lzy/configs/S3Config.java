package ai.lzy.configs;

import io.micronaut.context.annotation.ConfigurationProperties;
import javax.annotation.Nullable;

@ConfigurationProperties("s3")
public record S3Config(
    @Nullable String accessKey,
    @Nullable String secretKey,
    @Nullable String endpoint,
    @Nullable String region
) { }
