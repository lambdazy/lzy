package ai.lzy.service.storage;

import ai.lzy.longrunning.Operation;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.service.config.LzyServiceConfig;
import ai.lzy.service.config.StorageConfig;
import ai.lzy.v1.common.LMST;
import ai.lzy.v1.storage.LSS.GetStorageCredentialsRequest;
import ai.lzy.v1.storage.LSS.GetStorageCredentialsResponse;
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
import jakarta.annotation.Nullable;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;

import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.util.grpc.ProtoConverter.toProto;

@Singleton
@Requires(property = "lzy-service.storage.s3.memory.enabled", value = "true")
public class InMemoryS3Storage implements StorageService {
    private static final Logger LOG = LogManager.getLogger(InMemoryS3Storage.class);

    private final String endpoint;
    private final S3Mock server;
    private final AmazonS3 client;

    private final OperationDao operationDao;

    public InMemoryS3Storage(LzyServiceConfig config, StorageConfig.S3Credentials.InMemoryS3Credentials s3Config,
                             @Named("LzyServiceOperationDao") OperationDao operationDao)
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
    public void processCreateStorageOperation(String userId, String bucket, Operation operation) {
        LOG.debug("InMemoryS3Storage::createBucket, userId={}, bucket={}", userId, bucket);

        try {
            if (!client.doesBucketExistV2(bucket)) {
                client.createBucket(bucket);
            }
        } catch (SdkClientException e) {
            LOG.error("AWS SDK error while creating bucket '{}' for '{}': {}", bucket, userId, e.getMessage(), e);

            var errorStatus = Status.INTERNAL.withDescription("S3 internal error: " + e.getMessage()).withCause(e);

            try {
                operationDao.failOperation(operation.id(), toProto(errorStatus), null, LOG);
                operation.completeWith(errorStatus);
            } catch (SQLException ex) {
                LOG.error("Cannot fail operation {}: {}", operation.id(), ex.getMessage());
            }

            return;
        }

        var response = Any.pack(LMST.S3Credentials.newBuilder()
            .setEndpoint(endpoint)
            .build());

        try {
            withRetries(LOG, () -> operationDao.complete(operation.id(), response, null));
            operation.completeWith(response);
        } catch (Exception ex) {
            LOG.error("Error while executing transaction: {}", ex.getMessage(), ex);
            var errorStatus = Status.INTERNAL.withDescription("Error while executing request: " + ex.getMessage());

            try {
                operationDao.failOperation(operation.id(), toProto(errorStatus), null, LOG);
                operation.completeWith(errorStatus);
            } catch (SQLException e) {
                LOG.error("Cannot fail operation {}: {}", operation.id(), ex.getMessage());
            }
        }
    }

    @Override
    public void deleteStorage(@Nullable String userId, String bucket) {
        LOG.debug("InMemoryS3Storage::deleteBucket, bucket={}", bucket);

        try {
            if (client.doesBucketExistV2(bucket)) {
                client.deleteBucket(bucket);
            }
        } catch (SdkClientException e) {
            LOG.error("Can not delete bucket '{}': {}", bucket, e.getMessage(), e);
        }
    }

    @Override
    public void getStorageCreds(GetStorageCredentialsRequest request,
                                StreamObserver<GetStorageCredentialsResponse> response)
    {
        LOG.debug("InMemoryS3Storage::getBucketCredentials, userId={}, bucket={}",
            request.getUserId(), request.getBucket());

        response.onNext(GetStorageCredentialsResponse.newBuilder()
            .setAmazon(LMST.S3Credentials.newBuilder()
                .setEndpoint(endpoint)
                .build())
            .build());
        response.onCompleted();
    }
}
