package ai.lzy.test.impl;

import ai.lzy.iam.authorization.credentials.JwtCredentials;
import ai.lzy.util.ChannelBuilder;
import ai.lzy.model.grpc.ClientHeaderInterceptor;
import ai.lzy.model.grpc.GrpcHeaders;
import ai.lzy.v1.LzyStorageGrpc;
import ai.lzy.storage.LzyStorage;
import ai.lzy.storage.StorageConfig;
import ai.lzy.test.LzyStorageTestContext;
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
import java.io.Reader;
import java.io.StringReader;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static ai.lzy.model.utils.JwtCredentials.buildJWT;

@SuppressWarnings("UnstableApiUsage")
public class StorageThreadContext implements LzyStorageTestContext {
    private static final Duration STORAGE_STARTUP_TIME = Duration.ofSeconds(10);

    public static final int STORAGE_PORT = 7780;
    public static final int S3_PORT = 18081;

    private final HostAndPort iamAddress;
    private LzyStorage storage;
    private LzyStorageGrpc.LzyStorageBlockingStub client;

    public StorageThreadContext(HostAndPort iamAddress) {
        this.iamAddress = iamAddress;
    }

    @Override
    public HostAndPort address() {
        return HostAndPort.fromParts("localhost", STORAGE_PORT);
    }

    @Override
    public LzyStorageGrpc.LzyStorageBlockingStub client() {
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

        props.put("storage.address", "localhost:" + STORAGE_PORT);
        props.put("storage.iam.address", iamAddress.toString());
        props.put("storage.s3.memory.enabled", "true");
        props.put("storage.s3.memory.port", S3_PORT);

        JwtCredentials internalUserCreds;

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
            internalUserCreds = internalUserCredentials(config.iam().internal());
        }

        var channel = ChannelBuilder.forAddress(address())
            .usePlaintext()
            .enableRetry(LzyStorageGrpc.SERVICE_NAME)
            .build();

        client = LzyStorageGrpc.newBlockingStub(channel)
            .withWaitForReady()
            .withDeadlineAfter(STORAGE_STARTUP_TIME.getSeconds(), TimeUnit.SECONDS)
            .withInterceptors(ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION, internalUserCreds::token));

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

    private JwtCredentials internalUserCredentials(StorageConfig.IamInternal internal) {
        try (final Reader reader = new StringReader(internal.credentialPrivateKey())) {
            return new JwtCredentials(buildJWT(internal.userName(), reader));
        } catch (IOException | NoSuchAlgorithmException | InvalidKeySpecException e) {
            throw new RuntimeException("Cannot build credentials: " + e.getMessage(), e);
        }
    }
}
