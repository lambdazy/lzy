package ru.yandex.cloud.ml.platform.lzy.server.configs;

import io.micronaut.context.annotation.ConfigurationProperties;

@ConfigurationProperties("server")
public class ServerConfig {
    private String serverUri;
    private String whiteboardUrl;
    private String baseEnvDefaultImage;

    private YcCredentials yc;

    private ThreadAllocator threadAllocator;
    private KuberAllocator kuberAllocator;

    public String getBaseEnvDefaultImage() {
        return baseEnvDefaultImage;
    }

    public void setBaseEnvDefaultImage(String baseEnvDefaultImage) {
        this.baseEnvDefaultImage = baseEnvDefaultImage;
    }

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

    public KuberAllocator getKuberAllocator() {
        return kuberAllocator;
    }

    public void setKuberAllocator(KuberAllocator kuberAllocator) {
        this.kuberAllocator = kuberAllocator;
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

    @ConfigurationProperties("threadAllocator")
    public static class ThreadAllocator {
        private boolean enabled = false;
        private String filePath;
        private String servantClassName = "ru.yandex.cloud.ml.platform.lzy.servant.BashApi";

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public String getFilePath() {
            return filePath;
        }

        public void setFilePath(String filePath) {
            this.filePath = filePath;
        }

        public String getServantClassName() {
            return servantClassName;
        }

        public void setServantClassName(String servantClassName) {
            this.servantClassName = servantClassName;
        }
    }

    @ConfigurationProperties("kuberAllocator")
    public static class KuberAllocator {
        private boolean enabled = false;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }
    }
}
