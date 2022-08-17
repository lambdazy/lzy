package ai.lzy.storage.impl;

import ai.lzy.v1.Lzy;
import ai.lzy.v1.LzyStorageApi.*;
import ai.lzy.v1.LzyStorageGrpc;
import ai.lzy.storage.StorageConfig;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.google.common.net.HostAndPort;
import io.findify.s3mock.S3Mock;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.annotation.Requires;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@Singleton
@Requires(property = "storage.s3.memory.enabled", value = "true")
public class InMemoryS3Storage extends LzyStorageGrpc.LzyStorageImplBase {
    private static final Logger LOG = LogManager.getLogger(InMemoryS3Storage.class);

    private final String endpoint;
    private final S3Mock server;
    private final AmazonS3 client;

    @SuppressWarnings("UnstableApiUsage")
    public InMemoryS3Storage(StorageConfig config, StorageConfig.S3Credentials.InMemoryS3Credentials s3Config) {
        var storageAddress = HostAndPort.fromString(config.address());
        this.endpoint = "http://" + storageAddress.getHost() + ":" + s3Config.port();

        LOG.info("Starting in-memory s3 on {}", endpoint);

        this.server = new S3Mock.Builder()
            .withPort(s3Config.port())
            .withInMemoryBackend()
            .build();

        this.server.start();

        this.client = AmazonS3ClientBuilder.standard()
            .withPathStyleAccessEnabled(true)
            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, "us-west-1"))
            .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
            .build();
    }

    @PreDestroy
    public void shutdown() {
        server.shutdown();
    }

    @Override
    public void createS3Bucket(CreateS3BucketRequest request, StreamObserver<CreateS3BucketResponse> response) {
        LOG.debug("InMemoryS3Storage::createBucket, userId={}, bucket={}", request.getUserId(), request.getBucket());

        try {
            if (!client.doesBucketExistV2(request.getBucket())) {
                client.createBucket(request.getBucket());
            }
        } catch (SdkClientException e) {
            LOG.error("Can not create bucket '{}' for user '{}': {}",
                request.getBucket(), request.getUserId(), e.getMessage(), e);
            response.onError(Status.INTERNAL.withCause(e).asException());
        }

        response.onNext(CreateS3BucketResponse.newBuilder()
            .setAmazon(Lzy.AmazonCredentials.newBuilder()
                .setEndpoint(endpoint)
                .build())
            .build());
        response.onCompleted();
    }

    @Override
    public void deleteS3Bucket(DeleteS3BucketRequest request, StreamObserver<DeleteS3BucketResponse> response) {
        LOG.debug("InMemoryS3Storage::deleteBucket, bucket={}", request.getBucket());

        try {
            if (client.doesBucketExistV2(request.getBucket())) {
                client.deleteBucket(request.getBucket());
            }
        } catch (SdkClientException e) {
            LOG.error("Can not delete bucket '{}': {}", request.getBucket(), e.getMessage(), e);
            response.onError(Status.INTERNAL.withCause(e).asException());
        }

        response.onNext(DeleteS3BucketResponse.getDefaultInstance());
        response.onCompleted();
    }

    @Override
    public void getS3BucketCredentials(GetS3BucketCredentialsRequest request,
                                       StreamObserver<GetS3BucketCredentialsResponse> response) {
        LOG.debug("InMemoryS3Storage::getBucketCredentials, userId={}, bucket={}",
            request.getUserId(), request.getBucket());

        response.onNext(GetS3BucketCredentialsResponse.newBuilder()
            .setAmazon(Lzy.AmazonCredentials.newBuilder()
                .setEndpoint(endpoint)
                .build())
            .build());
        response.onCompleted();
    }
}
