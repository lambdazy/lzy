package ai.lzy.channelmanager;

import io.micronaut.context.annotation.ConfigurationProperties;

@ConfigurationProperties("channel-manager")
public record ChannelManagerConfig(
    int port,
    String whiteboardAddress
) {}
