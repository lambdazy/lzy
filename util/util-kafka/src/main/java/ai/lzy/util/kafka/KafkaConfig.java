package ai.lzy.util.kafka;

import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
public final class KafkaConfig {
    private boolean enabled = false;
    private List<String> bootstrapServers;

    private boolean tlsEnabled = false;
    private String tlsTruststorePath;
    private String tlsTruststorePassword;

    private String scramUsername;
    private String scramPassword;
}
