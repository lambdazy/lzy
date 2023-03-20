package ai.lzy.tunnel.config;

import io.micronaut.context.annotation.ConfigurationProperties;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@ConfigurationProperties("tunnel-agent")
public class ServiceConfig {
    private String address;
}
