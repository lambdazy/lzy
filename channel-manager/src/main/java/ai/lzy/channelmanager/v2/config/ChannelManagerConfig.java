package ai.lzy.channelmanager.v2.config;

import ai.lzy.iam.config.IamClientConfiguration;
import ai.lzy.model.db.DatabaseConfiguration;
import io.micronaut.context.annotation.ConfigurationBuilder;
import io.micronaut.context.annotation.ConfigurationProperties;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@ConfigurationProperties("channel-manager")
public final class ChannelManagerConfig {
    private String address;
    private String whiteboardAddress;
    private boolean stubIam = false;
    private String stubSlotApiAddress = null;
    private int lockBucketsCount = 256;

    @ConfigurationBuilder("iam")
    private final IamClientConfiguration iam = new IamClientConfiguration();

    @ConfigurationBuilder("database")
    private final DatabaseConfiguration database = new DatabaseConfiguration();

    @ConfigurationBuilder("connections")
    private final ConnectionManagerConfig connections = new ConnectionManagerConfig();

    @Override
    public String toString() {
        return "ChannelManagerConfig{" +
               "address='" + address + '\'' +
               ", stubIam=" + stubIam +
               ", stubSlotApiAddress=" + stubSlotApiAddress +
               ", lockBucketsCount=" + lockBucketsCount +
               ", iam=" + iam +
               ", database=" + database +
               ", connections=" + connections +
               '}';
    }
}
