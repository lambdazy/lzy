package ai.lzy.test;

import ai.lzy.priv.v2.LzyStorageGrpc;
import com.amazonaws.services.s3.AmazonS3;
import com.google.common.net.HostAndPort;

@SuppressWarnings("UnstableApiUsage")
public interface LzyStorageTestContext extends AutoCloseable {
    HostAndPort address();

    LzyStorageGrpc.LzyStorageBlockingStub client();

    AmazonS3 s3(String endpoint);

    void init();

    void close();
}
