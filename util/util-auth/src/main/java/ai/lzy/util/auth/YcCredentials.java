package ai.lzy.util.auth;

import io.micronaut.context.annotation.ConfigurationProperties;

@ConfigurationProperties("yc-credentials")
public record YcCredentials(
    String serviceAccountId,
    String keyId,
    String publicKey,
    String privateKey,
    String folderId
) {}
