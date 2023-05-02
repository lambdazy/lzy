package ai.lzy.storage.yc;

import ai.lzy.longrunning.Operation;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.DbOperation;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.storage.StorageService;
import ai.lzy.storage.config.StorageConfig;
import ai.lzy.storage.data.StorageDataSource;
import ai.lzy.util.auth.YcIamClient;
import ai.lzy.v1.common.LMST;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.storage.LSS.*;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CanonicalGrantee;
import com.amazonaws.services.s3.model.Permission;
import com.google.protobuf.Any;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.annotation.Requires;
import jakarta.annotation.Nullable;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.sql.SQLException;

import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.util.grpc.ProtoConverter.toProto;

@Singleton
@Requires(property = "storage.yc.enabled", value = "true")
@Requires(property = "storage.s3.yc.enabled", value = "true")
public class YandexCloudS3Storage implements StorageService {
    private static final Logger LOG = LogManager.getLogger(YandexCloudS3Storage.class);

    private final StorageConfig.S3Credentials.YcS3Credentials s3Creds;
    private final StorageConfig.YcCredentials ycCreds;
    private final StorageDataSource dataSource;

    private final OperationDao operationDao;

    public YandexCloudS3Storage(StorageConfig.S3Credentials.YcS3Credentials s3, StorageConfig.YcCredentials yc,
                                StorageDataSource dataSource, @Named("StorageOperationDao") OperationDao operationDao)
    {
        this.s3Creds = s3;
        this.ycCreds = yc;
        this.dataSource = dataSource;
        this.operationDao = operationDao;
    }

    @Override
    public void processCreateStorageOperation(CreateStorageRequest request, Operation operation,
                                             StreamObserver<LongRunning.Operation> responseObserver)
    {
        var userId = request.getUserId();
        var bucketName = request.getBucket();

        LOG.debug("YandexCloudS3Storage::createBucket, userId={}, bucket={}", userId, bucketName);

        var client = s3Client();

        try {
            if (!client.doesBucketExistV2(request.getBucket())) {
                client.createBucket(request.getBucket());
            }
        } catch (SdkClientException e) {
            LOG.error("AWS SDK error while creating bucket '{}' for '{}': {}",
                request.getBucket(), request.getUserId(), e.getMessage(), e);

            var errorStatus = Status.INTERNAL.withDescription("S3 internal error: " + e.getMessage()).withCause(e);

            try {
                operationDao.failOperation(operation.id(), toProto(errorStatus), null, LOG);
            } catch (SQLException ex) {
                LOG.error("Cannot fail operation {}: {}", operation.id(), ex.getMessage());
            }

            responseObserver.onError(errorStatus.asRuntimeException());
            return;
        }

        final String[] tokens = {/* service_account */ null, /* access_token */ null, /* secret_token */ null};

        // todo: ssokolvyak -- add with retries, but pull createServiceAccount call out from transaction
        //  or make it idempotent
        try (var transaction = TransactionHandle.create(dataSource)) {
            DbOperation.execute(transaction, dataSource, conn -> {
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
                        return;
                    }
                }

                String[] newTokens;
                try {
                    newTokens = createServiceAccountForUser(request.getUserId(), request.getBucket());
                } catch (Exception e) {
                    throw new SQLException(e);
                }

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
            });

            transaction.commit();
        } catch (SQLException e) {
            LOG.error("SQL error while creating bucket '{}' for '{}': {}",
                request.getBucket(), request.getUserId(), e.getMessage(), e);
            safeDeleteBucket(request.getUserId(), request.getBucket(), client);

            var errorStatus = Status.INTERNAL.withDescription("SQL error: " + e.getMessage()).withCause(e);

            try {
                operationDao.failOperation(operation.id(), toProto(errorStatus), null, LOG);
            } catch (SQLException ex) {
                LOG.error("Cannot fail operation {}: {}", operation.id(), ex.getMessage());
            }

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

            var errorStatus = Status.INTERNAL.withDescription("S3 internal error: " + e.getMessage()).withCause(e);

            try {
                operationDao.failOperation(operation.id(), toProto(errorStatus), null, LOG);
            } catch (SQLException ex) {
                LOG.error("Cannot fail operation {}: {}", operation.id(), ex.getMessage());
            }

            responseObserver.onError(errorStatus.asRuntimeException());
            return;
        }

        var response = Any.pack(CreateStorageResponse.newBuilder()
            .setS3(LMST.S3Credentials.newBuilder()
                .setEndpoint(s3Creds.getEndpoint())
                .setAccessToken(tokens[1])
                .setSecretToken(tokens[2])
                .build())
            .build());

        try {
            var completedOp = withRetries(LOG, () -> operationDao.complete(operation.id(), response, null));

            responseObserver.onNext(completedOp.toProto());
            responseObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error("Error while executing transaction: {}", ex.getMessage(), ex);
            var errorStatus = Status.INTERNAL.withDescription("Error while executing request: " + ex.getMessage());

            try {
                operationDao.failOperation(operation.id(), toProto(errorStatus), null, LOG);
            } catch (SQLException e) {
                LOG.error("Cannot fail operation {}: {}", operation.id(), ex.getMessage());
            }

            responseObserver.onError(errorStatus.asRuntimeException());
        }
    }

    @Override
    public void deleteStorage(DeleteStorageRequest request, StreamObserver<DeleteStorageResponse> response) {
        LOG.debug("YandexCloudS3Storage::deleteBucket, bucket={}", request.getBucket());
        safeDeleteBucket(null, request.getBucket(), s3Client());

        response.onNext(DeleteStorageResponse.getDefaultInstance());
        response.onCompleted();
    }

    @Override
    public void getStorageCreds(GetStorageCredentialsRequest request,
                                StreamObserver<GetStorageCredentialsResponse> response)
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
                response.onNext(GetStorageCredentialsResponse.newBuilder()
                    .setAmazon(LMST.S3Credentials.newBuilder()
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
