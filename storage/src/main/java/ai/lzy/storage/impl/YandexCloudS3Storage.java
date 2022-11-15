package ai.lzy.storage.impl;

import ai.lzy.longrunning.Operation;
import ai.lzy.longrunning.Operation.IdempotencyKey;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.Transaction;
import ai.lzy.model.db.exceptions.DaoException;
import ai.lzy.storage.BeanFactory;
import ai.lzy.storage.config.StorageConfig;
import ai.lzy.storage.data.StorageDataSource;
import ai.lzy.util.auth.YcIamClient;
import ai.lzy.util.grpc.GrpcHeaders;
import ai.lzy.v1.common.LMS3;
import ai.lzy.v1.longrunning.LongRunning;
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
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.sql.SQLException;
import javax.annotation.Nullable;

import static ai.lzy.longrunning.RequestHash.md5;
import static ai.lzy.longrunning.dao.OperationDao.OPERATION_IDEMPOTENCY_KEY_CONSTRAINT;
import static ai.lzy.model.db.DbHelper.isUniqueViolation;
import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.util.grpc.ProtoConverter.toProto;

@Singleton
@Requires(property = "storage.yc.enabled", value = "true")
@Requires(property = "storage.s3.yc.enabled", value = "true")
public class YandexCloudS3Storage extends LzyStorageServiceGrpc.LzyStorageServiceImplBase {
    private static final Logger LOG = LogManager.getLogger(YandexCloudS3Storage.class);

    private final StorageConfig.S3Credentials.YcS3Credentials s3Creds;
    private final StorageConfig.YcCredentials ycCreds;
    private final StorageDataSource dataSource;

    private final OperationDao operationsDao;

    public YandexCloudS3Storage(StorageConfig.S3Credentials.YcS3Credentials s3, StorageConfig.YcCredentials yc,
                                StorageDataSource dataSource, @Named(BeanFactory.DAO_NAME) OperationDao operationDao)
    {
        this.s3Creds = s3;
        this.ycCreds = yc;
        this.dataSource = dataSource;
        this.operationsDao = operationDao;
    }

    @Override
    public void createS3Bucket(CreateS3BucketRequest request, StreamObserver<LongRunning.Operation> responseObserver) {
        var userId = request.getUserId();
        var bucketName = request.getBucket();

        LOG.debug("YandexCloudS3Storage::createBucket, userId={}, bucket={}", userId, bucketName);

        final var idempotencyToken = GrpcHeaders.getIdempotencyKey();
        IdempotencyKey idempotencyKey = null;

        if (idempotencyToken != null) {
            idempotencyKey = new IdempotencyKey(idempotencyToken, md5(request));
            if (loadExistingOp(idempotencyKey, responseObserver)) {
                return;
            }
        }

        final var op = new Operation(userId, "Create S3 bucket: name=" + bucketName, Any.getDefaultInstance());

        try {
            withRetries(LOG, () -> operationsDao.create(op, null));
        } catch (Exception ex) {
            if (idempotencyKey != null && isUniqueViolation(ex, OPERATION_IDEMPOTENCY_KEY_CONSTRAINT)) {
                if (loadExistingOp(idempotencyKey, responseObserver)) {
                    return;
                }

                LOG.error("Idempotency key {} not found", idempotencyToken);
                responseObserver.onError(Status.INTERNAL.withDescription("Idempotency key conflict").asException());
                return;
            }

            LOG.error("Cannot create operation for s3 bucket creation: { bucketName: {}, userId: {} }, error: {}",
                bucketName, userId, ex.getMessage(), ex);
            responseObserver.onError(Status.INTERNAL.withDescription(ex.getMessage()).asException());
            return;
        }

        var client = s3Client();

        Status errorStatus = null;

        try {
            if (!client.doesBucketExistV2(request.getBucket())) {
                client.createBucket(request.getBucket());
            } else {
                errorStatus = Status.ALREADY_EXISTS.withDescription("Bucket '" + request.getBucket() +
                    "' already exists");
            }
        } catch (SdkClientException e) {
            LOG.error("AWS SDK error while creating bucket '{}' for '{}': {}",
                request.getBucket(), request.getUserId(), e.getMessage(), e);

            errorStatus = Status.INTERNAL.withDescription("S3 internal error: " + e.getMessage()).withCause(e);
        }

        try {
            if (errorStatus != null) {
                var statusProto = toProto(errorStatus);
                withRetries(LOG, () -> operationsDao.updateError(op.id(), statusProto.toByteArray(), null));

                responseObserver.onError(errorStatus.asRuntimeException());
                return;
            }

            try {
                client.setBucketLifecycleConfiguration(request.getBucket(),
                    new BucketLifecycleConfiguration()
                        .withRules(new BucketLifecycleConfiguration.Rule().withExpirationInDays(30)));
            } catch (SdkClientException e) {
                LOG.error("AWS SDK error while creating bucket '{}' for '{}': {}",
                    request.getBucket(), request.getUserId(), e.getMessage(), e);

                errorStatus = Status.INTERNAL.withDescription("S3 internal error: " + e.getMessage()).withCause(e);

                try {
                    client.deleteBucket(request.getBucket());
                } catch (Exception ee) {
                    LOG.error("Cannot remove s3 bucket '{}': {}", request.getBucket(), e.getMessage(), e);
                }
            }

            if (errorStatus != null) {
                var statusProto = toProto(errorStatus);
                withRetries(LOG, () -> operationsDao.updateError(op.id(), statusProto.toByteArray(), null));

                responseObserver.onError(errorStatus.asRuntimeException());
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
                        st.setString(1, request.getUserId());

                        var rs = st.executeQuery();
                        if (rs.next()) {
                            tokens[0] = rs.getString("service_account");
                            tokens[1] = rs.getString("access_token");
                            tokens[2] = rs.getString("secret_token");
                            return true;
                        }
                    }

                    var newTokens = createServiceAccountForUser(request.getUserId(), request.getBucket());

                    try (var st = conn.prepareStatement("""
                        insert into yc_s3_credentials (user_id, service_account, access_token, secret_token)
                        values (?, ?, ?, ?)"""))
                    {
                        st.setString(1, request.getUserId());
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
                LOG.error("SQL error while creating bucket '{}' for '{}': {}",
                    request.getBucket(), request.getUserId(), e.getMessage(), e);
                safeDeleteBucket(request.getUserId(), request.getBucket(), client);

                errorStatus = Status.INTERNAL.withDescription("SQL error: " + e.getMessage()).withCause(e);
            }

            if (errorStatus != null) {
                var statusProto = toProto(errorStatus);
                withRetries(LOG, () -> operationsDao.updateError(op.id(), statusProto.toByteArray(), null));

                responseObserver.onError(errorStatus.asRuntimeException());
                return;
            }

            try {
                var acl = client.getBucketAcl(request.getBucket());
                var grantee = new CanonicalGrantee(/* service_account */ tokens[0]);
                acl.grantPermission(grantee, Permission.FullControl);
                client.setBucketAcl(request.getBucket(), acl);
            } catch (SdkClientException e) {
                LOG.error("AWS SDK error while creating ACL at bucket '{}' for user '{}': {}. Delete bucket.",
                    request.getBucket(), request.getUserId(), e.getMessage(), e);
                safeDeleteBucket(request.getUserId(), request.getBucket(), client);

                errorStatus = Status.INTERNAL.withDescription("S3 internal error: " + e.getMessage()).withCause(e);
            }

            if (errorStatus != null) {
                var statusProto = toProto(errorStatus);
                withRetries(LOG, () -> operationsDao.updateError(op.id(), statusProto.toByteArray(), null));

                responseObserver.onError(errorStatus.asRuntimeException());
                return;
            }

            var response = Any.pack(CreateS3BucketResponse.newBuilder()
                .setAmazon(LMS3.AmazonS3Endpoint.newBuilder()
                    .setEndpoint(s3Creds.getEndpoint())
                    .setAccessToken(tokens[1])
                    .setSecretToken(tokens[2])
                    .build())
                .build());
            var completedOp = withRetries(LOG, () ->
                operationsDao.updateResponse(op.id(), response.toByteArray(), null));

            responseObserver.onNext(completedOp.toProto());
            responseObserver.onCompleted();

        } catch (Exception ex) {
            LOG.error("Error while executing transaction: {}", ex.getMessage(), ex);
            var status = Status.INTERNAL.withDescription("Error while executing request: " + ex.getMessage());
            failOperation(op.id(), toProto(status));

            responseObserver.onError(status.asRuntimeException());
        }
    }

    private boolean loadExistingOp(ai.lzy.longrunning.Operation.IdempotencyKey idempotencyKey,
                                   StreamObserver<LongRunning.Operation> responseObserver)
    {
        try {
            var op = withRetries(LOG, () -> operationsDao.getByIdempotencyKey(idempotencyKey.token(), null));
            if (op != null) {
                if (!idempotencyKey.equals(op.idempotencyKey())) {
                    LOG.error("Idempotency key {} conflict", idempotencyKey.token());
                    responseObserver.onError(Status.INVALID_ARGUMENT
                        .withDescription("IdempotencyKey conflict").asException());
                    return true;
                }

                responseObserver.onNext(op.toProto());
                responseObserver.onCompleted();
                return true;
            }

            return false; // key doesn't exist
        } catch (Exception ex) {
            LOG.error("Cannot create session: {}", ex.getMessage(), ex);
            responseObserver.onError(Status.INTERNAL.withDescription(ex.getMessage()).asException());
            return true;
        }
    }

    private void failOperation(String operationId, com.google.rpc.Status error) {
        try {
            var op = withRetries(LOG, () -> operationsDao.updateError(operationId, error.toByteArray(), null));
            if (op == null) {
                LOG.error("Cannot fail operation {} with reason {}: operation not found",
                    operationId, error.getMessage());
            }
        } catch (Exception ex) {
            LOG.error("Cannot fail operation {} with reason {}: {}",
                operationId, error.getMessage(), ex.getMessage(), ex);
        }
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

        return new String[]{serviceAccountId, credentials.getAWSAccessKeyId(), credentials.getAWSSecretKey()};
    }
}
