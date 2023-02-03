package ai.lzy.site;

import ai.lzy.iam.config.IamClientConfiguration;
import io.micronaut.context.annotation.ConfigurationBuilder;
import io.micronaut.context.annotation.ConfigurationProperties;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@ConfigurationProperties("site")
public class ServiceConfig {
    private String schedulerAddress;
    private String hostname = "https://lzy.ai:8443";  // Hostname to redirect github to

    @ConfigurationBuilder("iam")
    private final IamClientConfiguration iam = new IamClientConfiguration();

    @Getter
    @Setter
    @ConfigurationProperties("github-credentials")
    public static final class GithubCredentials {
        private String clientId;
        private String clientSecret;
    }

    @Getter
    @Setter
    @ConfigurationProperties("github")
    public static final class Github {
        private String address;
        private String apiAddress;
    }
}
