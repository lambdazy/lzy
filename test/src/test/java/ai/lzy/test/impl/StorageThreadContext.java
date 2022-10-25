package ai.lzy.test.impl;

import ai.lzy.storage.LzyStorage;
import ai.lzy.storage.StorageConfig;
import ai.lzy.test.LzyStorageTestContext;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.util.grpc.ClientHeaderInterceptor;
import ai.lzy.util.grpc.GrpcHeaders;
import ai.lzy.v1.storage.LzyStorageServiceGrpc;
import ai.lzy.whiteboard.api.SnapshotApi;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.google.common.net.HostAndPort;
import io.grpc.ConnectivityState;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.PropertySource;
import org.apache.logging.log4j.LogManager;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

@SuppressWarnings("UnstableApiUsage")
public class StorageThreadContext implements LzyStorageTestContext {
    private static final Duration STORAGE_STARTUP_TIME = Duration.ofSeconds(10);

    public static final int STORAGE_PORT = 7780;
    public static final int S3_PORT = 18081;

    private final HostAndPort iamAddress;
    private LzyStorage storage;
    private LzyStorageServiceGrpc.LzyStorageServiceBlockingStub client;

    public StorageThreadContext(HostAndPort iamAddress) {
        this.iamAddress = iamAddress;
    }

    @Override
    public HostAndPort address() {
        return HostAndPort.fromParts("localhost", STORAGE_PORT);
    }

    @Override
    public LzyStorageServiceGrpc.LzyStorageServiceBlockingStub client() {
        return client.withInterceptors();
    }

    @Override
    public AmazonS3 s3(String endpoint) {
        return AmazonS3ClientBuilder.standard()
            .withPathStyleAccessEnabled(true)
            .withEndpointConfiguration(
                new AwsClientBuilder.EndpointConfiguration(endpoint, "us-west-1"))
            .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
            .build();
    }

    @Override
    public void init() {
        var props = Utils.loadModuleTestProperties("storage");
        props.putAll(Utils.createModuleDatabase("storage"));

        props.put("storage.address", "localhost:" + STORAGE_PORT);
        props.put("storage.iam.address", iamAddress.toString());
        props.put("storage.s3.memory.enabled", "true");
        props.put("storage.s3.memory.port", S3_PORT);

        RenewableJwt internalUserCreds;

        try (ApplicationContext context = ApplicationContext.run(PropertySource.of(props))) {
            var logger = LogManager.getLogger(SnapshotApi.class);
            logger.info("Starting LzyStorage on port {}...", STORAGE_PORT);

            try {
                storage = new LzyStorage(context);
                storage.start();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            var config = context.getBean(StorageConfig.class);
            internalUserCreds = config.getIam().createRenewableToken();
        }

        var channel = ChannelBuilder.forAddress(address())
            .usePlaintext()
            .enableRetry(LzyStorageServiceGrpc.SERVICE_NAME)
            .build();

        client = LzyStorageServiceGrpc.newBlockingStub(channel)
            .withWaitForReady()
            .withDeadlineAfter(STORAGE_STARTUP_TIME.getSeconds(), TimeUnit.SECONDS)
            .withInterceptors(ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION,
                                                             () -> internalUserCreds.get().token()));

        while (channel.getState(true) != ConnectivityState.READY) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
        }
    }

    @Override
    public void close() {
        try {
            storage.close(false);
            storage.awaitTermination();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
