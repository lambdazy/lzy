package ai.lzy.storage.test;

import ai.lzy.longrunning.Operation;
import ai.lzy.storage.WithInMemoryOperationStorage;
import ai.lzy.storage.config.StorageConfig;
import ai.lzy.util.grpc.GrpcHeaders;
import ai.lzy.v1.common.LMS3;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.storage.LSS.*;
import ai.lzy.v1.storage.LzyStorageServiceGrpc;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.google.common.net.HostAndPort;
import com.google.protobuf.Any;
import io.findify.s3mock.S3Mock;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.annotation.Requires;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Singleton
@Requires(property = "storage.s3.memory.enabled", value = "true")
public class InMemoryS3Storage extends LzyStorageServiceGrpc.LzyStorageServiceImplBase implements
    WithInMemoryOperationStorage
{
    private static final Logger LOG = LogManager.getLogger(InMemoryS3Storage.class);

    private final String endpoint;
    private final S3Mock server;
    private final AmazonS3 client;

    private final Map<String, Operation> operations = new ConcurrentHashMap<>();

    public InMemoryS3Storage(StorageConfig config, StorageConfig.S3Credentials.InMemoryS3Credentials s3Config) {
        var storageAddress = HostAndPort.fromString(config.getAddress());
        this.endpoint = "http://" + storageAddress.getHost() + ":" + s3Config.getPort();

        LOG.info("Starting in-memory s3 on {}", endpoint);

        this.server = new S3Mock.Builder()
            .withPort(s3Config.getPort())
            .withInMemoryBackend()
            .build();

        this.server.start();

        this.client = AmazonS3ClientBuilder.standard()
            .withPathStyleAccessEnabled(true)
            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, "us-west-1"))
            .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
            .build();
    }

    @Override
    public Map<String, Operation> getOperations() {
        return operations;
    }

    @PreDestroy
    public void shutdown() {
        server.shutdown();
    }

    @Override
    public void createS3Bucket(CreateS3BucketRequest request, StreamObserver<LongRunning.Operation> response) {
        LOG.debug("InMemoryS3Storage::createBucket, userId={}, bucket={}", request.getUserId(), request.getBucket());

        var operation = new Operation("userId-" + request.getUserId(), "Create bucket: " + request.getBucket(),
            Any.getDefaultInstance());

        operations.put(GrpcHeaders.getHeader(GrpcHeaders.IDEMPOTENCY_KEY), operation);

        try {
            if (!client.doesBucketExistV2(request.getBucket())) {
                client.createBucket(request.getBucket());
            }
        } catch (SdkClientException e) {
            LOG.error("Can not create bucket '{}' for user '{}': {}",
                request.getBucket(), request.getUserId(), e.getMessage(), e);
            operation.setError(Status.INTERNAL.withDescription("Cannot create bucket: " + e.getMessage()));

            response.onNext(operation.toProto());
            response.onCompleted();
            return;
        }

        operation.setResponse(Any.pack(CreateS3BucketResponse.newBuilder()
            .setAmazon(LMS3.AmazonS3Endpoint.newBuilder()
                .setEndpoint(endpoint))
            .build()));
        response.onNext(operation.toProto());
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
                                       StreamObserver<GetS3BucketCredentialsResponse> response)
    {
        LOG.debug("InMemoryS3Storage::getBucketCredentials, userId={}, bucket={}",
            request.getUserId(), request.getBucket());

        response.onNext(GetS3BucketCredentialsResponse.newBuilder()
            .setAmazon(LMS3.AmazonS3Endpoint.newBuilder()
                .setEndpoint(endpoint)
                .build())
            .build());
        response.onCompleted();
    }
}
