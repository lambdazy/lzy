package ai.lzy.kharon;

import ai.lzy.model.db.DatabaseConfiguration;
import io.micronaut.context.annotation.ConfigurationBuilder;
import io.micronaut.context.annotation.ConfigurationProperties;
import lombok.Getter;
import lombok.Setter;

import javax.annotation.Nullable;

@Getter
@Setter
@ConfigurationProperties("kharon")
public class KharonConfig {
    private String address;
    @Nullable
    private String externalHost;
    private String serverAddress;
    private String whiteboardAddress;
    private String snapshotAddress;
    private String channelManagerAddress;
    private int servantProxyPort;
    private int servantFsProxyPort;
    private int channelManagerProxyPort;

    @ConfigurationBuilder("database")
    private final DatabaseConfiguration database = new DatabaseConfiguration();
}
