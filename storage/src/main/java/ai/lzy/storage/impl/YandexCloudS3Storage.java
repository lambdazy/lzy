package ai.lzy.storage.impl;

import ai.lzy.longrunning.Operation;
import ai.lzy.model.db.Transaction;
import ai.lzy.model.db.exceptions.DaoException;
import ai.lzy.storage.WithInMemoryOperationStorage;
import ai.lzy.storage.config.StorageConfig;
import ai.lzy.storage.data.StorageDataSource;
import ai.lzy.util.auth.YcIamClient;
import ai.lzy.util.grpc.GrpcHeaders;
import ai.lzy.v1.common.LMS3;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.longrunning.LongRunning.MessageChecksum;
import ai.lzy.v1.storage.LSS.*;
import ai.lzy.v1.storage.LzyStorageServiceGrpc;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration;
import com.amazonaws.services.s3.model.CanonicalGrantee;
import com.amazonaws.services.s3.model.Permission;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;

@Singleton
@Requires(property = "storage.yc.enabled", value = "true")
@Requires(property = "storage.s3.yc.enabled", value = "true")
public class YandexCloudS3Storage extends LzyStorageServiceGrpc.LzyStorageServiceImplBase implements
    WithInMemoryOperationStorage
{
    private static final Logger LOG = LogManager.getLogger(YandexCloudS3Storage.class);

    private final MessageDigest md5Encoder;

    private final StorageConfig.S3Credentials.YcS3Credentials s3Creds;
    private final StorageConfig.YcCredentials ycCreds;
    private final StorageDataSource dataSource;

    private final Map<String, Operation> operations = new ConcurrentHashMap<>();

    public YandexCloudS3Storage(StorageConfig.S3Credentials.YcS3Credentials s3, StorageConfig.YcCredentials yc,
                                StorageDataSource dataSource) throws NoSuchAlgorithmException
    {
        this.md5Encoder = MessageDigest.getInstance("MD5");

        this.s3Creds = s3;
        this.ycCreds = yc;
        this.dataSource = dataSource;
    }

    @Override
    public Map<String, Operation> getOperations() {
        return operations;
    }

    @Override
    public void createS3Bucket(CreateS3BucketRequest request, StreamObserver<LongRunning.Operation> response) {
        var userId = request.getUserId();
        var bucketName = request.getBucket();

        LOG.debug("YandexCloudS3Storage::createBucket, userId={}, bucket={}", userId, bucketName);

        String idempotencyKey = GrpcHeaders.getHeader(GrpcHeaders.IDEMPOTENCY_KEY);

        byte[] checksum = md5Encoder.digest(request.toByteArray());

        var newOperation = new Operation("userId-" + userId,
            "Create S3 bucket with: { userId: %s, bucketName: %s }".formatted(userId, bucketName),
            Any.pack(MessageChecksum.newBuilder().setMd5Digest(ByteString.copyFrom(checksum)).build()));

        Operation operation = Objects.requireNonNull(operations.putIfAbsent(idempotencyKey, newOperation));

        if (operation.id().contentEquals(newOperation.id())) {
            createS3Bucket(request, operation);
        } else {
            byte[] expectedChecksum;
            try {
                expectedChecksum = operation.meta().unpack(MessageChecksum.class).getMd5Digest().toByteArray();
            } catch (InvalidProtocolBufferException e) {
                LOG.error("Cannot obtain checksum value for operation: { operationId: {} }, error: {}",
                    operation.id(), e.getMessage());
                response.onError(Status.INTERNAL.withDescription("Cannot obtain operation").asRuntimeException());
                return;
            }

            if (!Arrays.equals(checksum, expectedChecksum)) {
                var message = "Idempotency key already exists with other arguments";
                LOG.error(message);
                response.onError(Status.FAILED_PRECONDITION.withDescription(message).asRuntimeException());
                return;
            }
        }

        response.onNext(operation.toProto());
        response.onCompleted();
    }

    private void createS3Bucket(CreateS3BucketRequest request, Operation operation) {
        var bucketName = request.getBucket();
        var userId = request.getUserId();

        var client = s3Client();

        try {
            if (!client.doesBucketExistV2(bucketName)) {
                client.createBucket(bucketName);
            } else {
                LOG.error("Attempt to create bucket with name which already exists: { bucketName: {} }", bucketName);
                operation.setError(Status.ALREADY_EXISTS.withDescription("Bucket '" + bucketName + "' already exists"));
                return;
            }
        } catch (SdkClientException e) {
            LOG.error("AWS SDK error while creating bucket '{}' for '{}': {}", bucketName, userId, e.getMessage(), e);
            operation.setError(Status.INTERNAL.withDescription("S3 internal error: " + e.getMessage()));
            return;
        }

        try {
            client.setBucketLifecycleConfiguration(bucketName,
                new BucketLifecycleConfiguration()
                    .withRules(new BucketLifecycleConfiguration.Rule().withExpirationInDays(30)));
        } catch (SdkClientException e) {
            LOG.error("AWS SDK error while creating bucket '{}' for '{}': {}", bucketName, userId, e.getMessage(), e);
            operation.setError(Status.INTERNAL.withDescription("S3 internal error: " + e.getMessage()));

            safeDeleteBucket(userId, request.getBucket(), client);
            return;
        }

        final String[] tokens = {/* service_account */ null, /* access_token */ null, /* secret_token */ null};

        try {
            Transaction.execute(dataSource, conn -> {
                try (var st = conn.prepareStatement("""
                    select service_account, access_token, secret_token
                    from yc_s3_credentials
                    where user_id = ?
                    for update"""))
                {
                    st.setString(1, userId);

                    var rs = st.executeQuery();
                    if (rs.next()) {
                        tokens[0] = rs.getString("service_account");
                        tokens[1] = rs.getString("access_token");
                        tokens[2] = rs.getString("secret_token");
                        return true;
                    }
                }

                var newTokens = createServiceAccountForUser(userId, bucketName);

                try (var st = conn.prepareStatement("""
                    insert into yc_s3_credentials (user_id, service_account, access_token, secret_token)
                    values (?, ?, ?, ?)"""))
                {
                    st.setString(1, userId);
                    st.setString(2, newTokens[0]);
                    st.setString(3, newTokens[1]);
                    st.setString(4, newTokens[2]);
                    st.executeUpdate();
                }

                tokens[0] = newTokens[0];
                tokens[1] = newTokens[1];
                tokens[2] = newTokens[2];
                return true;
            });
        } catch (DaoException e) {
            LOG.error("SQL error while creating bucket '{}' for '{}': {}", bucketName, userId, e.getMessage(), e);
            operation.setError(Status.INTERNAL.withDescription("SQL error: " + e.getMessage()));

            safeDeleteBucket(userId, bucketName, client);
            return;
        }

        try {
            var acl = client.getBucketAcl(bucketName);
            var grantee = new CanonicalGrantee(/* service_account */ tokens[0]);
            acl.grantPermission(grantee, Permission.FullControl);
            client.setBucketAcl(bucketName, acl);
        } catch (SdkClientException e) {
            LOG.error("AWS SDK error while creating ACL at bucket '{}' for user '{}': {}. Delete bucket.", bucketName,
                userId, e.getMessage(), e);
            operation.setError(Status.INTERNAL.withDescription("S3 internal error: " + e.getMessage()));

            safeDeleteBucket(userId, bucketName, client);
            return;
        }

        operation.setResponse(Any.pack(CreateS3BucketResponse.newBuilder()
            .setAmazon(LMS3.AmazonS3Endpoint.newBuilder()
                .setEndpoint(s3Creds.getEndpoint())
                .setAccessToken(tokens[1])
                .setSecretToken(tokens[2]))
            .build()));
    }

    @Override
    public void deleteS3Bucket(DeleteS3BucketRequest request, StreamObserver<DeleteS3BucketResponse> response) {
        LOG.debug("YandexCloudS3Storage::deleteBucket, bucket={}", request.getBucket());
        safeDeleteBucket(null, request.getBucket(), s3Client());

        response.onNext(DeleteS3BucketResponse.getDefaultInstance());
        response.onCompleted();
    }

    @Override
    public void getS3BucketCredentials(GetS3BucketCredentialsRequest request,
                                       StreamObserver<GetS3BucketCredentialsResponse> response)
    {
        LOG.debug("YandexCloudS3Storage::getBucketCredentials, userId={}, bucket={}",
            request.getUserId(), request.getBucket());

        try (var conn = dataSource.connect();
             var st = conn.prepareStatement("""
                 select access_token, secret_token
                 from yc_s3_credentials
                 where user_id = ?"""))
        {
            st.setString(1, request.getUserId());

            var rs = st.executeQuery();
            if (rs.next()) {
                response.onNext(GetS3BucketCredentialsResponse.newBuilder()
                    .setAmazon(LMS3.AmazonS3Endpoint.newBuilder()
                        .setEndpoint(s3Creds.getEndpoint())
                        .setAccessToken(rs.getString("access_token"))
                        .setSecretToken(rs.getString("secret_token"))
                        .build())
                    .build());
                response.onCompleted();
                return;
            }

            response.onError(Status.NOT_FOUND.asException());
        } catch (SQLException e) {
            response.onError(Status.INTERNAL
                .withDescription(e.getMessage()).withCause(e).asException());
        }
    }

    private AmazonS3 s3Client() {
        return AmazonS3ClientBuilder.standard()
            .withCredentials(new AWSStaticCredentialsProvider(
                new BasicAWSCredentials(s3Creds.getAccessToken(), s3Creds.getSecretToken())))
            .withEndpointConfiguration(
                new AmazonS3ClientBuilder.EndpointConfiguration(s3Creds.getEndpoint(), "us-west-1"))
            .withPathStyleAccessEnabled(true)
            .build();
    }

    private void safeDeleteBucket(@Nullable String userId, String bucket, AmazonS3 client) {
        LOG.info("About to delete bucket '{}' for user '{}'", bucket, userId);
        try {
            client.deleteBucket(bucket);
        } catch (SdkClientException e) {
            LOG.error("AWS SDK error while deleting bucket '{}' for user '{}': {}", bucket, userId, e.getMessage(), e);
        }
    }

    private String[] createServiceAccountForUser(String userId, String bucket)
        throws IOException, InterruptedException
    {
        CloseableHttpClient httpclient = HttpClients.createDefault();

        String serviceAccountId = YcIamClient.createServiceAccount(userId, RenewableToken.getToken(), httpclient,
            ycCreds.getFolderId(), bucket);

        AWSCredentials credentials = YcIamClient.createStaticCredentials(serviceAccountId,
            RenewableToken.getToken(), httpclient);

        return new String[] {serviceAccountId, credentials.getAWSAccessKeyId(), credentials.getAWSSecretKey()};
    }
}
