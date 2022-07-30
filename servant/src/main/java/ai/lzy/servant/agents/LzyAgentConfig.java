package ai.lzy.servant.agents;

import java.net.URI;
import java.nio.file.Path;

public class LzyAgentConfig {
    private final URI serverAddress;
    private final URI whiteboardAddress;
    private final URI channelManagerAddress;
    private final String scheme;
    private final String agentHost;
    private final String token;
    private final Path root;
    private final String user;
    private final String agentId;
    private final String bucket;
    private final int agentPort;
    private final int fsPort;

    private LzyAgentConfig(URI serverAddress, URI whiteboardAddress, String agentHost, int agentPort, int fsPort,
                           String token, Path root, String user, String agentId, String bucket,
                           URI channelManagerAddress, String scheme) {
        this.serverAddress = serverAddress;
        this.whiteboardAddress = whiteboardAddress;
        this.agentHost = agentHost;
        this.agentPort = agentPort;
        this.fsPort = fsPort;
        this.token = token;
        this.root = root;
        this.user = user;
        this.agentId = agentId;
        this.bucket = bucket;
        this.channelManagerAddress = channelManagerAddress;
        this.scheme = scheme;
    }

    public static LzyAgentConfig updateAgentId(LzyAgentConfig config, String servantId) {
        return new LzyAgentConfig(config.serverAddress, config.whiteboardAddress, config.agentHost,
            config.agentPort, config.fsPort, config.token, config.root, config.user, servantId, config.bucket,
            config.channelManagerAddress, config.scheme);
    }

    public static LzyAgentConfigBuilder builder() {
        return new LzyAgentConfigBuilder();
    }

    public String getBucket() {
        return bucket;
    }

    public URI getServerAddress() {
        return serverAddress;
    }

    public URI getWhiteboardAddress() {
        return whiteboardAddress;
    }

    public String getAgentHost() {
        return agentHost;
    }

    public String getToken() {
        return token;
    }

    public Path getRoot() {
        return root;
    }

    public String getUser() {
        return user;
    }

    public String getAgentId() {
        return agentId;
    }

    public int getAgentPort() {
        return agentPort;
    }

    public int getFsPort() {
        return fsPort;
    }

    public URI getChannelManagerAddress() {
        return channelManagerAddress;
    }

    public String getScheme() {
        return scheme;
    }

    public static class LzyAgentConfigBuilder {
        private URI serverAddress;
        private URI whiteboardAddress;
        private String agentHost;
        private String token;
        private Path root;
        private String user;
        private String servantId;
        private String bucket;
        private int agentPort;
        private int fsPort;
        private URI channelManagerAddress;
        private String scheme;

        public LzyAgentConfigBuilder agentHost(String agentHost) {
            this.agentHost = agentHost;
            return this;
        }

        public LzyAgentConfigBuilder agentPort(int agentPort) {
            this.agentPort = agentPort;
            return this;
        }

        public LzyAgentConfigBuilder fsPort(int fsPort) {
            this.fsPort = fsPort;
            return this;
        }

        public LzyAgentConfigBuilder root(Path root) {
            this.root = root;
            return this;
        }

        public LzyAgentConfigBuilder serverAddress(URI serverAddress) {
            this.serverAddress = serverAddress;
            return this;
        }

        public LzyAgentConfigBuilder whiteboardAddress(URI whiteboardAddress) {
            this.whiteboardAddress = whiteboardAddress;
            return this;
        }

        public LzyAgentConfigBuilder servantId(String task) {
            this.servantId = task;
            return this;
        }

        public LzyAgentConfigBuilder bucket(String bucket) {
            this.bucket = bucket;
            return this;
        }

        public LzyAgentConfigBuilder token(String token) {
            this.token = token;
            return this;
        }

        public LzyAgentConfigBuilder user(String user) {
            this.user = user;
            return this;
        }

        public LzyAgentConfigBuilder channelManagerAddress(URI channelManagerAddress) {
            this.channelManagerAddress = channelManagerAddress;
            return this;
        }

        public LzyAgentConfigBuilder scheme(String scheme) {
            this.scheme = scheme;
            return this;
        }

        public LzyAgentConfig build() {
            return new LzyAgentConfig(serverAddress, whiteboardAddress, agentHost, agentPort, fsPort, token, root, user,
                servantId, bucket, channelManagerAddress, scheme);
        }
    }
}
