package ai.lzy.util.kafka;

import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
public final class KafkaConfig {
    private boolean enabled = false;
    private List<String> bootstrapServers;

    private Encrypt encrypt = new Encrypt();

    private ScramAuth scramAuth = new ScramAuth();

    @Getter
    @Setter
    public static final class Encrypt {
        private boolean enabled = false;
        private String truststorePath;
        private String truststorePassword;
    }

    @Getter
    @Setter
    public static final class ScramAuth {
        private String username;
        private String password;
    }
}
