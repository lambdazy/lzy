package ai.lzy.storage;

import java.time.Duration;

public class StorageClientConfiguration {
    private String address;
    private Duration bucketCreationTimeout;

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public Duration getBucketCreationTimeout() {
        return bucketCreationTimeout;
    }

    public void setBucketCreationTimeout(Duration bucketCreationTimeout) {
        this.bucketCreationTimeout = bucketCreationTimeout;
    }
}
