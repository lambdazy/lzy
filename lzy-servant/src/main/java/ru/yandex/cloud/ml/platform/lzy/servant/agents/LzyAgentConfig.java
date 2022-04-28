package ru.yandex.cloud.ml.platform.lzy.servant.agents;

import java.net.URI;
import java.nio.file.Path;

public class LzyAgentConfig {
    private final URI serverAddress;
    private final URI whiteboardAddress;
    private final String agentName;
    private final String agentInternalName;
    private final String token;
    private final Path root;
    private final String user;
    private final String servantId;
    private final String bucket;
    private final int agentPort;
    private final int fsPort;

    private LzyAgentConfig(URI serverAddress, URI whiteboardAddress, String agentName, String agentInternalName,
                           String token, Path root, String user, String servantId, int agentPort, int fsPort,
                           String bucket) {
        this.serverAddress = serverAddress;
        this.whiteboardAddress = whiteboardAddress;
        this.agentName = agentName;
        this.agentInternalName = agentInternalName;
        this.token = token;
        this.root = root;
        this.user = user;
        this.servantId = servantId;
        this.agentPort = agentPort;
        this.fsPort = fsPort;
        this.bucket = bucket;
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

    public String getAgentName() {
        return agentName;
    }

    public String getAgentInternalName() {
        return agentInternalName;
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

    public String getServantId() {
        return servantId;
    }

    public int getAgentPort() {
        return agentPort;
    }

    public int getFsPort() {
        return fsPort;
    }

    public static class LzyAgentConfigBuilder {
        private URI serverAddress;
        private URI whiteboardAddress;
        private String agentName;
        private String agentInternalName;
        private String token;
        private Path root;
        private String user;
        private String servantId;
        private String bucket;
        private int agentPort;
        private int fsPort;

        public LzyAgentConfigBuilder agentInternalName(String agentInternalName) {
            this.agentInternalName = agentInternalName;
            return this;
        }

        public LzyAgentConfigBuilder agentName(String agentName) {
            this.agentName = agentName;
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

        public LzyAgentConfig build() {
            return new LzyAgentConfig(serverAddress, whiteboardAddress, agentName, agentInternalName, token, root, user,
                servantId, agentPort, fsPort, bucket);
        }
    }
}
