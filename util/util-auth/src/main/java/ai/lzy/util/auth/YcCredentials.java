package ai.lzy.util.auth;

public record YcCredentials(
    String serviceAccountId,
    String keyId,
    String publicKey,
    String privateKey,
    String folderId
) {}
