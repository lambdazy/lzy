package ai.lzy.kharon;

import ai.lzy.iam.config.IamClientConfiguration;
import ai.lzy.model.db.DatabaseConfiguration;
import ai.lzy.storage.config.StorageClientConfiguration;
import io.micronaut.context.annotation.ConfigurationBuilder;
import io.micronaut.context.annotation.ConfigurationProperties;

import javax.annotation.Nullable;

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

    @ConfigurationBuilder("storage")
    private final StorageClientConfiguration storage = new StorageClientConfiguration();

    @ConfigurationBuilder("workflow")
    private final WorkflowConfig workflow = new WorkflowConfig();

    @ConfigurationBuilder("iam")
    private final IamClientConfiguration iam = new IamClientConfiguration();


    public static class WorkflowConfig {
        private boolean enabled;

        public boolean enabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }
    }

    public String address() {
        return address;
    }

    @Nullable
    public String externalHost() {
        return externalHost;
    }

    public String serverAddress() {
        return serverAddress;
    }

    public String whiteboardAddress() {
        return whiteboardAddress;
    }

    public String snapshotAddress() {
        return snapshotAddress;
    }

    public String channelManagerAddress() {
        return channelManagerAddress;
    }

    public int servantProxyPort() {
        return servantProxyPort;
    }

    public int servantFsProxyPort() {
        return servantFsProxyPort;
    }

    public int channelManagerProxyPort() {
        return channelManagerProxyPort;
    }

    public DatabaseConfiguration database() {
        return database;
    }

    public IamClientConfiguration iam() {
        return iam;
    }

    public StorageClientConfiguration storage() {
        return storage;
    }

    public WorkflowConfig workflow() {
        return workflow;
    }

    // micronaut specific boilerplate

    public void setAddress(String address) {
        this.address = address;
    }

    public void setExternalHost(@Nullable String externalHost) {
        this.externalHost = externalHost;
    }

    public void setServerAddress(String serverAddress) {
        this.serverAddress = serverAddress;
    }

    public void setWhiteboardAddress(String whiteboardAddress) {
        this.whiteboardAddress = whiteboardAddress;
    }

    public void setSnapshotAddress(String snapshotAddress) {
        this.snapshotAddress = snapshotAddress;
    }

    public void setChannelManagerAddress(String channelManagerAddress) {
        this.channelManagerAddress = channelManagerAddress;
    }

    public void setServantProxyPort(int servantProxyPort) {
        this.servantProxyPort = servantProxyPort;
    }

    public void setServantFsProxyPort(int servantFsProxyPort) {
        this.servantFsProxyPort = servantFsProxyPort;
    }

    public void setChannelManagerProxyPort(int channelManagerProxyPort) {
        this.channelManagerProxyPort = channelManagerProxyPort;
    }

    public IamClientConfiguration getIam() {
        return iam;
    }

    public DatabaseConfiguration getDatabase() {
        return database;
    }

    public StorageClientConfiguration getStorage() {
        return storage;
    }

    public WorkflowConfig getWorkflow() {
        return workflow;
    }
}
