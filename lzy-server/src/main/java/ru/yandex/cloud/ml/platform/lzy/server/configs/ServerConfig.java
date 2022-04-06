package ru.yandex.cloud.ml.platform.lzy.server.configs;

import io.micronaut.context.annotation.ConfigurationProperties;

@ConfigurationProperties("server")
public class ServerConfig {
    private String serverUri;

    private String whiteboardUrl;

    private YcCredentials yc;

    private ThreadAllocator threadAllocator;

    public ThreadAllocator getThreadAllocator() {
        return threadAllocator;
    }

    public void setThreadAllocator(ThreadAllocator threadAllocator) {
        this.threadAllocator = threadAllocator;
    }

    private int userLimit;

    public String getServerUri() {
        return serverUri;
    }

    public void setServerUri(String serverUri) {
        this.serverUri = serverUri;
    }

    public YcCredentials getYc() {
        return yc;
    }

    public void setYc(YcCredentials yc) {
        this.yc = yc;
    }

    public String getWhiteboardUrl() {
        return whiteboardUrl;
    }

    public void setWhiteboardUrl(String whiteboardUrl) {
        this.whiteboardUrl = whiteboardUrl;
    }

    public int getUserLimit() {
        return userLimit;
    }

    public void setUserLimit(int userLimit) {
        this.userLimit = userLimit;
    }

    @ConfigurationProperties("yc")
    public static class YcCredentials {

        private boolean enabled = false;

        private String serviceAccountId;

        private String keyId;

        private String privateKey;

        private String folderId;

        public String getFolderId() {
            return folderId;
        }

        public void setFolderId(String folderId) {
            this.folderId = folderId;
        }

        public String getPrivateKey() {
            return privateKey;
        }

        public void setPrivateKey(String privateKey) {
            this.privateKey = privateKey;
        }

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public String getServiceAccountId() {
            return serviceAccountId;
        }

        public void setServiceAccountId(String serviceAccountId) {
            this.serviceAccountId = serviceAccountId;
        }

        public String getKeyId() {
            return keyId;
        }

        public void setKeyId(String keyId) {
            this.keyId = keyId;
        }
    }

    @ConfigurationProperties("allocator.thread")
    public static class ThreadAllocator {
        private boolean enabled = false;
        private String jarPath;
        private String servantClassName = "ru.yandex.cloud.ml.platform.lzy.servant.BashApi";

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public String getJarPath() {
            return jarPath;
        }

        public void setJarPath(String jarPath) {
            this.jarPath = jarPath;
        }

        public String getServantClassName() {
            return servantClassName;
        }

        public void setServantClassName(String servantClassName) {
            this.servantClassName = servantClassName;
        }
    }
}
