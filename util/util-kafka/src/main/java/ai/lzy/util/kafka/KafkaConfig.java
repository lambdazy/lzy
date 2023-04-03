package ai.lzy.util.kafka;

import jakarta.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
public final class KafkaConfig {
    private boolean enabled = false;
    private List<String> bootstrapServers;
    private Encrypt encrypt;
    @Nullable
    private ScramAuth scramAuth = null;

    @Getter
    @Setter
    public static final class Encrypt {
        private String keystorePath;
        private String keystorePassword;
    }

    @Getter
    @Setter
    public static final class ScramAuth {
        private String username;
        private String password;
    }
}
