package ai.lzy.storage.test;

import ai.lzy.longrunning.Operation;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.storage.BeanFactory;
import ai.lzy.storage.StorageService;
import ai.lzy.storage.config.StorageConfig;
import ai.lzy.v1.common.LMS3;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.storage.LSS.*;
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
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.util.grpc.ProtoConverter.toProto;

@Singleton
@Requires(property = "storage.s3.memory.enabled", value = "true")
public class InMemoryS3Storage implements StorageService {
    private static final Logger LOG = LogManager.getLogger(InMemoryS3Storage.class);

    private final String endpoint;
    private final S3Mock server;
    private final AmazonS3 client;

    private final OperationDao operationDao;

    public InMemoryS3Storage(StorageConfig config, StorageConfig.S3Credentials.InMemoryS3Credentials s3Config,
                             @Named(BeanFactory.DAO_NAME) OperationDao operationDao)
    {
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

        this.operationDao = operationDao;
    }

    @PreDestroy
    public void shutdown() {
        server.shutdown();
    }

    @Override
    public void processCreateBucketOperation(CreateS3BucketRequest request, Operation operation,
                                             StreamObserver<LongRunning.Operation> responseObserver)
    {
        var userId = request.getUserId();
        var bucketName = request.getBucket();

        LOG.debug("InMemoryS3Storage::createBucket, userId={}, bucket={}", userId, bucketName);

        try {
            if (!client.doesBucketExistV2(request.getBucket())) {
                client.createBucket(request.getBucket());
            }
        } catch (SdkClientException e) {
            LOG.error("AWS SDK error while creating bucket '{}' for '{}': {}",
                request.getBucket(), request.getUserId(), e.getMessage(), e);

            var errorStatus = Status.INTERNAL.withDescription("S3 internal error: " + e.getMessage()).withCause(e);

            OperationDao.failOperation(operationDao, operation.id(), toProto(errorStatus), LOG);

            responseObserver.onError(errorStatus.asRuntimeException());
            return;
        }

        var response = Any.pack(CreateS3BucketResponse.newBuilder()
            .setAmazon(LMS3.AmazonS3Endpoint.newBuilder()
                .setEndpoint(endpoint)
                .build())
            .build());

        try {
            var completedOp = withRetries(LOG, () ->
                operationDao.updateResponse(operation.id(), response.toByteArray(), null));

            responseObserver.onNext(completedOp.toProto());
            responseObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error("Error while executing transaction: {}", ex.getMessage(), ex);
            var errorStatus = Status.INTERNAL.withDescription("Error while executing request: " + ex.getMessage());

            OperationDao.failOperation(operationDao, operation.id(), toProto(errorStatus), LOG);

            responseObserver.onError(errorStatus.asRuntimeException());
        }
    }

    @Override
    public void deleteBucket(DeleteS3BucketRequest request, StreamObserver<DeleteS3BucketResponse> response) {
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
    public void getBucketCreds(GetS3BucketCredentialsRequest request,
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
