package ai.lzy.iam.configs;

import ai.lzy.model.db.DatabaseConfiguration;
import io.micronaut.context.annotation.ConfigurationBuilder;
import io.micronaut.context.annotation.ConfigurationProperties;

@ConfigurationProperties("iam")
public class ServiceConfig {
    private int userLimit;
    private int serverPort;

    @ConfigurationBuilder("database")
    private final DatabaseConfiguration database = new DatabaseConfiguration();

    public int getUserLimit() {
        return userLimit;
    }

    public void setUserLimit(int userLimit) {
        this.userLimit = userLimit;
    }

    public int getServerPort() {
        return serverPort;
    }

    public void setServerPort(int serverPort) {
        this.serverPort = serverPort;
    }

    public DatabaseConfiguration getDatabase() {
        return database;
    }
}
