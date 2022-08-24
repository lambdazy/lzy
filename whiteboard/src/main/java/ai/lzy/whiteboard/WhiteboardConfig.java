package ai.lzy.whiteboard;

import ai.lzy.iam.config.IamClientConfiguration;
import io.micronaut.context.annotation.ConfigurationBuilder;
import io.micronaut.context.annotation.ConfigurationProperties;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@ConfigurationProperties("whiteboard")
public class WhiteboardConfig {
    private String address;

    @ConfigurationBuilder("iam")
    private final IamClientConfiguration iam = new IamClientConfiguration();

    @Getter
    @Setter
    @ConfigurationProperties("database")
    public static final class DbConfig {
        private String url;
        private String username;
        private String password;
        private int minPoolSize;
        private int maxPoolSize;
    }
}
