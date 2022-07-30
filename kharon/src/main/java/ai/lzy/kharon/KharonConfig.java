package ai.lzy.kharon;

import io.micronaut.context.annotation.ConfigurationProperties;

import javax.annotation.Nullable;

@ConfigurationProperties("kharon")
public record KharonConfig(
    String address,
    @Nullable
    String externalHost,
    String serverAddress,
    String whiteboardAddress,
    String snapshotAddress,
    String channelManagerAddress,
    int servantProxyPort,
    int servantFsProxyPort,
    int channelManagerProxyPort,
    DatabaseConfig database,
    IamConfig iam,
    StorageConfig storage,
    WorkflowConfig workflow
) {

    @ConfigurationProperties("database")
    public record DatabaseConfig(
        String url,
        String username,
        String password,
        int minPoolSize,
        int maxPoolSize
    ) {}

    @ConfigurationProperties("iam")
    public record IamConfig(
        String address,
        IamInternal internal
    ) {}

    @ConfigurationProperties("iam.internal")
    public record IamInternal(
        String userName,
        String credentialPrivateKey
    ) {}

    @ConfigurationProperties("storage")
    public record StorageConfig(
        String address
    ) {}

    @ConfigurationProperties("workflow")
    public record WorkflowConfig(
        boolean enabled
    ) {}
}
