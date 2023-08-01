package ai.lzy.test.context.config;

import ai.lzy.test.GrpcUtils;
import io.micronaut.context.annotation.ConfigurationProperties;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@ConfigurationProperties("lzy-config")
@Getter
@Setter
public class LzyConfig {
    Configs configs;
    Environments environments;
    Ports ports;
    Database database;

    @ConfigurationProperties("configs")
    @Getter
    @Setter
    public static final class Configs {
        String iamConfig;
        String allocatorConfig;
        String channelManagerConfig;
        String graphExecutorConfig;
        String schedulerConfig;
        String whiteboardConfig;
        String lzyServiceConfig;

        public Configs() {}

        public Configs(Builder builder) {
            this.iamConfig = builder.iamConfig;
            this.allocatorConfig = builder.allocatorConfig;
            this.channelManagerConfig = builder.channelManagerConfig;
            this.graphExecutorConfig = builder.graphExecutorConfig;
            this.schedulerConfig = builder.schedulerConfig;
            this.whiteboardConfig = builder.whiteboardConfig;
            this.lzyServiceConfig = builder.lzyServiceConfig;
        }

        public List<String> asCmdArgs() {
            var prefix = "-lzy-config.configs.";
            var cmdArgs = new ArrayList<String>();
            if (iamConfig != null) {
                cmdArgs.add(prefix + "iam-config=" + iamConfig);
            }
            if (allocatorConfig != null) {
                cmdArgs.add(prefix + "allocator-config=" + allocatorConfig);
            }
            if (channelManagerConfig != null) {
                cmdArgs.add(prefix + "channel-manager-config=" + channelManagerConfig);
            }
            if (graphExecutorConfig != null) {
                cmdArgs.add(prefix + "graph-executor-config=" + graphExecutorConfig);
            }
            if (schedulerConfig != null) {
                cmdArgs.add(prefix + "scheduler-config=" + schedulerConfig);
            }
            if (whiteboardConfig != null) {
                cmdArgs.add(prefix + "whiteboard-config=" + whiteboardConfig);
            }
            if (lzyServiceConfig != null) {
                cmdArgs.add(prefix + "lzy-service-config=" + lzyServiceConfig);
            }
            return cmdArgs;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static final class Builder {
            private String iamConfig;
            private String allocatorConfig;
            private String channelManagerConfig;
            private String graphExecutorConfig;
            private String schedulerConfig;
            private String whiteboardConfig;
            private String lzyServiceConfig;

            public Builder setIamConfig(String iamConfig) {
                this.iamConfig = iamConfig;
                return this;
            }

            public Builder setAllocatorConfig(String allocatorConfig) {
                this.allocatorConfig = allocatorConfig;
                return this;
            }

            public Builder setChannelManagerConfig(String channelManagerConfig) {
                this.channelManagerConfig = channelManagerConfig;
                return this;
            }

            public Builder setGraphExecutorConfig(String graphExecutorConfig) {
                this.graphExecutorConfig = graphExecutorConfig;
                return this;
            }

            public Builder setSchedulerConfig(String schedulerConfig) {
                this.schedulerConfig = schedulerConfig;
                return this;
            }

            public Builder setWhiteboardConfig(String whiteboardConfig) {
                this.whiteboardConfig = whiteboardConfig;
                return this;
            }

            public Builder setLzyServiceConfig(String lzyServiceConfig) {
                this.lzyServiceConfig = lzyServiceConfig;
                return this;
            }

            public Configs build() {
                return new Configs(this);
            }
        }
    }

    @ConfigurationProperties("environments")
    @Getter
    @Setter
    public static final class Environments {
        List<String> iamEnvironments;
        List<String> allocatorEnvironments;
        List<String> channelManagerEnvironments;
        List<String> graphExecutorEnvironments;
        List<String> schedulerEnvironments;
        List<String> whiteboardEnvironments;
        List<String> lzyServiceEnvironments;

        public Environments() {}

        public Environments(Builder builder) {
            this.iamEnvironments = builder.iamEnvironments;
            this.allocatorEnvironments = builder.allocatorEnvironments;
            this.channelManagerEnvironments = builder.channelManagerEnvironments;
            this.graphExecutorEnvironments = builder.graphExecutorEnvironments;
            this.schedulerEnvironments = builder.schedulerEnvironments;
            this.whiteboardEnvironments = builder.whiteboardEnvironments;
            this.lzyServiceEnvironments = builder.lzyServiceEnvironments;
        }

        public List<String> asCmdArgs() {
            var prefix = "-lzy-config.environments.";
            var cmdArgs = new ArrayList<String>();

            if (iamEnvironments != null) {
                cmdArgs.add(prefix + "iam-environments=" + asCmdArg(iamEnvironments));
            }
            if (allocatorEnvironments != null) {
                cmdArgs.add(prefix + "allocator-environments=" + asCmdArg(allocatorEnvironments));
            }
            if (channelManagerEnvironments != null) {
                cmdArgs.add(prefix + "channel-manager-environments=" + asCmdArg(channelManagerEnvironments));
            }
            if (graphExecutorEnvironments != null) {
                cmdArgs.add(prefix + "graph-executor-environments=" + asCmdArg(graphExecutorEnvironments));
            }
            if (schedulerEnvironments != null) {
                cmdArgs.add(prefix + "scheduler-environments=" + asCmdArg(schedulerEnvironments));
            }
            if (whiteboardEnvironments != null) {
                cmdArgs.add(prefix + "whiteboard-environments=" + asCmdArg(whiteboardEnvironments));
            }
            if (lzyServiceEnvironments != null) {
                cmdArgs.add(prefix + "lzy-service-environments=" + asCmdArg(lzyServiceEnvironments));
            }
            return cmdArgs;
        }

        private static String asCmdArg(Collection<String> collection) {
            return String.join(",", collection);
        }

        public static Builder builder() {
            return new Builder();
        }

        public static final class Builder {
            private List<String> iamEnvironments;
            private List<String> allocatorEnvironments;
            private List<String> channelManagerEnvironments;
            private List<String> graphExecutorEnvironments;
            private List<String> schedulerEnvironments;
            private List<String> whiteboardEnvironments;
            private List<String> lzyServiceEnvironments;

            public Builder addIamEnvironment(String iamEnvironment) {
                if (iamEnvironments == null) {
                    iamEnvironments = new ArrayList<>();
                }
                iamEnvironments.add(iamEnvironment);
                return this;
            }

            public Builder addAllocatorEnvironment(String allocatorEnvironment) {
                if (allocatorEnvironments == null) {
                    allocatorEnvironments = new ArrayList<>();
                }
                allocatorEnvironments.add(allocatorEnvironment);
                return this;
            }

            public Builder addChannelManagerEnvironment(String channelManagerEnvironment) {
                if (channelManagerEnvironments == null) {
                    channelManagerEnvironments = new ArrayList<>();
                }
                channelManagerEnvironments.add(channelManagerEnvironment);
                return this;
            }

            public Builder addGraphExecutorEnvironment(String graphExecutorEnvironment) {
                if (graphExecutorEnvironments == null) {
                    graphExecutorEnvironments = new ArrayList<>();
                }
                graphExecutorEnvironments.add(graphExecutorEnvironment);
                return this;
            }

            public Builder addSchedulerEnvironment(String schedulerEnvironment) {
                if (schedulerEnvironments == null) {
                    schedulerEnvironments = new ArrayList<>();
                }
                schedulerEnvironments.add(schedulerEnvironment);
                return this;
            }

            public Builder addWhiteboardEnvironment(String whiteboardEnvironment) {
                if (whiteboardEnvironments == null) {
                    whiteboardEnvironments = new ArrayList<>();
                }
                whiteboardEnvironments.add(whiteboardEnvironment);
                return this;
            }

            public Builder addLzyServiceEnvironment(String lzyServiceEnvironment) {
                if (lzyServiceEnvironments == null) {
                    lzyServiceEnvironments = new ArrayList<>();
                }
                lzyServiceEnvironments.add(lzyServiceEnvironment);
                return this;
            }

            public Environments build() {
                return new Environments(this);
            }
        }
    }

    @ConfigurationProperties("ports")
    @Getter
    @Setter
    public static final class Ports {
        int iamPort;
        int allocatorPort;
        int channelManagerPort;
        int graphExecutorPort;
        int schedulerPort;
        int whiteboardPort;
        int lzyServicePort;

        public Ports() {}

        public Ports(int iamPort, int allocatorPort, int channelManagerPort, int graphExecutorPort, int schedulerPort,
                     int whiteboardPort, int lzyServicePort)
        {
            this.iamPort = iamPort;
            this.allocatorPort = allocatorPort;
            this.channelManagerPort = channelManagerPort;
            this.graphExecutorPort = graphExecutorPort;
            this.schedulerPort = schedulerPort;
            this.whiteboardPort = whiteboardPort;
            this.lzyServicePort = lzyServicePort;
        }

        public List<String> asCmdArgs() {
            var prefix = "-lzy-config.ports.";
            return List.of(
                prefix + "iam-port=" + iamPort,
                prefix + "allocator-port=" + allocatorPort,
                prefix + "channel-manager-port=" + channelManagerPort,
                prefix + "graph-executor-port=" + graphExecutorPort,
                prefix + "scheduler-port=" + schedulerPort,
                prefix + "whiteboard-port=" + whiteboardPort,
                prefix + "lzy-service-port=" + lzyServicePort
            );
        }

        public static Ports findFree() {
            return new Ports(
                GrpcUtils.rollPort(),
                GrpcUtils.rollPort(),
                GrpcUtils.rollPort(),
                GrpcUtils.rollPort(),
                GrpcUtils.rollPort(),
                GrpcUtils.rollPort(),
                GrpcUtils.rollPort()
            );
        }
    }

    @ConfigurationProperties("database")
    @Getter
    @Setter
    public static final class Database {
        public static final String DB_ENABLED = "database.enabled";
        public static final String DB_URL = "database.url";
        public static final String DB_USERNAME = "database.username";
        public static final String DB_PASSWORD = "database.password";
        public static final String POSTGRES_USERNAME = "postgres";
        public static final String POSTGRES_PASSWORD = "";

        String iamDbUrl;
        String allocatorDbUrl;
        String channelManagerDbUrl;
        String graphExecutorDbUrl;
        String schedulerDbUrl;
        String whiteboardDbUrl;
        String lzyServiceDbUrl;

        public Database() {}

        public Database(Builder builder) {
            this.iamDbUrl = builder.iamDbUrl;
            this.allocatorDbUrl = builder.allocatorDbUrl;
            this.channelManagerDbUrl = builder.channelManagerDbUrl;
            this.graphExecutorDbUrl = builder.graphExecutorDbUrl;
            this.schedulerDbUrl = builder.schedulerDbUrl;
            this.whiteboardDbUrl = builder.whiteboardDbUrl;
            this.lzyServiceDbUrl = builder.lzyServiceDbUrl;
        }

        public List<String> asCmdArg() {
            var prefix = "-lzy-config.database.";
            var cmdArgs = new ArrayList<String>();

            if (iamDbUrl != null) {
                cmdArgs.add(prefix + "iam-db-url=" + iamDbUrl);
            }
            if (allocatorDbUrl != null) {
                cmdArgs.add(prefix + "allocator-db-url=" + allocatorDbUrl);
            }
            if (channelManagerDbUrl != null) {
                cmdArgs.add(prefix + "channel-manager-db-url=" + channelManagerDbUrl);
            }
            if (graphExecutorDbUrl != null) {
                cmdArgs.add(prefix + "graph-executor-db-url=" + graphExecutorDbUrl);
            }
            if (schedulerDbUrl != null) {
                cmdArgs.add(prefix + "scheduler-db-url=" + schedulerDbUrl);
            }
            if (whiteboardDbUrl != null) {
                cmdArgs.add(prefix + "whiteboard-db-url=" + whiteboardDbUrl);
            }
            if (lzyServiceDbUrl != null) {
                cmdArgs.add(prefix + "lzy-service-db-url=" + lzyServiceDbUrl);
            }
            return cmdArgs;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static final class Builder {
            private String iamDbUrl;
            private String allocatorDbUrl;
            private String channelManagerDbUrl;
            private String graphExecutorDbUrl;
            private String schedulerDbUrl;
            private String whiteboardDbUrl;
            private String lzyServiceDbUrl;

            public Builder setIamDbUrl(String iamDbUrl) {
                this.iamDbUrl = iamDbUrl;
                return this;
            }

            public Builder setAllocatorDbUrl(String allocatorDbUrl) {
                this.allocatorDbUrl = allocatorDbUrl;
                return this;
            }

            public Builder setChannelManagerDbUrl(String channelManagerDbUrl) {
                this.channelManagerDbUrl = channelManagerDbUrl;
                return this;
            }

            public Builder setGraphExecutorDbUrl(String graphExecutorDbUrl) {
                this.graphExecutorDbUrl = graphExecutorDbUrl;
                return this;
            }

            public Builder setSchedulerDbUrl(String schedulerDbUrl) {
                this.schedulerDbUrl = schedulerDbUrl;
                return this;
            }

            public Builder setWhiteboardDbUrl(String whiteboardDbUrl) {
                this.whiteboardDbUrl = whiteboardDbUrl;
                return this;
            }

            public Builder setLzyServiceDbUrl(String lzyServiceDbUrl) {
                this.lzyServiceDbUrl = lzyServiceDbUrl;
                return this;
            }

            public Database build() {
                return new Database(this);
            }
        }
    }
}
