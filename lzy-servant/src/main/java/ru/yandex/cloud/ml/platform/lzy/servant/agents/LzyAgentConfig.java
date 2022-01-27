package ru.yandex.cloud.ml.platform.lzy.servant.agents;

import java.net.URI;
import java.nio.file.Path;

public class LzyAgentConfig {
    private LzyAgentConfig(URI serverAddress, URI whiteboardAddress, String agentName, String agentInternalName, String token, Path root, String user, String task, int agentPort, String bucket) {
        this.serverAddress = serverAddress;
        this.whiteboardAddress = whiteboardAddress;
        this.agentName = agentName;
        this.agentInternalName = agentInternalName;
        this.token = token;
        this.root = root;
        this.user = user;
        this.task = task;
        this.agentPort = agentPort;
        this.bucket = bucket;
    }

    private final URI serverAddress;
    private final URI whiteboardAddress;
    private final String agentName;
    private final String agentInternalName;
    private final String token;
    private final Path root;
    private final String user;
    private final String task;
    private final String bucket;
    private final int agentPort;

    public static LzyAgentConfigBuilder builder() {
        return new LzyAgentConfigBuilder();
    }

    public String getBucket() {
        return bucket;
    }

    public static class LzyAgentConfigBuilder {
        private URI serverAddress;
        private URI whiteboardAddress;
        private String agentName;
        private String agentInternalName;
        private String token;
        private Path root;
        private String user;
        private String task;
        private String bucket;
        private int agentPort;

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

        public LzyAgentConfigBuilder task(String task) {
            this.task = task;
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
            return new LzyAgentConfig(serverAddress, whiteboardAddress, agentName, agentInternalName, token, root, user, task, agentPort, bucket);
        }
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

    public String getTask() {
        return task;
    }

    public int getAgentPort() {
        return agentPort;
    }
}
