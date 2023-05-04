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
    private String tlsTruststoreType = "PKCS12";

    private String tlsKeystorePath;
    private String tlsKeystorePassword;
    private String tlsKeystoreType = "PKCS12";

    private String scramUsername;
    private String scramPassword;

    public static KafkaConfig of(String bootstrapServer) {
        var cfg = new KafkaConfig();
        cfg.enabled = true;
        cfg.bootstrapServers = List.of(bootstrapServer);
        return cfg;
    }
}
