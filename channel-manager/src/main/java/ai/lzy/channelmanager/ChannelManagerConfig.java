package ai.lzy.channelmanager;

import io.micronaut.context.annotation.ConfigurationProperties;

@ConfigurationProperties("channel-manager")
public record ChannelManagerConfig(
    String address,
    String whiteboardAddress,
    Iam iam
) {
    @ConfigurationProperties("iam")
    public record Iam(
        String address
    ) {}
}
