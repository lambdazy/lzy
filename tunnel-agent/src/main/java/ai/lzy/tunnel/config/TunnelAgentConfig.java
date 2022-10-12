package ai.lzy.tunnel.config;

import io.micronaut.context.annotation.ConfigurationProperties;

@ConfigurationProperties("tunnel-agent")
public record TunnelAgentConfig(
    String address
) {
}
