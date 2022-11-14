package ai.lzy.storage.impl;

import ai.lzy.model.db.Transaction;
import ai.lzy.model.db.exceptions.DaoException;
import ai.lzy.storage.config.StorageConfig;
import ai.lzy.storage.data.StorageDataSource;
import ai.lzy.util.auth.YcIamClient;
import ai.lzy.v1.common.LMS3;
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
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.sql.SQLException;
import javax.annotation.Nullable;

@Singleton
@Requires(property = "storage.yc.enabled", value = "true")
@Requires(property = "storage.s3.yc.enabled", value = "true")
public class YandexCloudS3Storage extends LzyStorageServiceGrpc.LzyStorageServiceImplBase {
    private static final Logger LOG = LogManager.getLogger(YandexCloudS3Storage.class);

    private final StorageConfig.S3Credentials.YcS3Credentials s3Creds;
    private final StorageConfig.YcCredentials ycCreds;
    private final StorageDataSource dataSource;

    public YandexCloudS3Storage(StorageConfig.S3Credentials.YcS3Credentials s3, StorageConfig.YcCredentials yc,
                                StorageDataSource dataSource) {
        this.s3Creds = s3;
        this.ycCreds = yc;
        this.dataSource = dataSource;
    }

    @Override
    public void createS3Bucket(CreateS3BucketRequest request, StreamObserver<CreateS3BucketResponse> response) {
        LOG.debug("YandexCloudS3Storage::createBucket, userId={}, bucket={}", request.getUserId(), request.getBucket());

        var client = s3Client();

        try {
            if (!client.doesBucketExistV2(request.getBucket())) {
                client.createBucket(request.getBucket());
            } else {
                response.onError(Status.ALREADY_EXISTS
                    .withDescription("Bucket '" + request.getBucket() + "' already exists").asException());
                return;
            }
        } catch (SdkClientException e) {
            LOG.error("AWS SDK error while creating bucket '{}' for '{}': {}",
                request.getBucket(), request.getUserId(), e.getMessage(), e);
            response.onError(Status.INTERNAL
                .withDescription("S3 internal error: " + e.getMessage()).withCause(e).asException());
            return;
        }

        try {
            client.setBucketLifecycleConfiguration(request.getBucket(),
                new BucketLifecycleConfiguration()
                    .withRules(new BucketLifecycleConfiguration.Rule().withExpirationInDays(30)));
        } catch (SdkClientException e) {
            LOG.error("AWS SDK error while creating bucket '{}' for '{}': {}",
                request.getBucket(), request.getUserId(), e.getMessage(), e);
            response.onError(Status.INTERNAL
                .withDescription("S3 internal error: " + e.getMessage()).withCause(e).asException());

            try {
                client.deleteBucket(request.getBucket());
            } catch (Exception ee) {
                LOG.error("Cannot remove s3 bucket '{}': {}", request.getBucket(), e.getMessage(), e);
            }

            return;
        }

        final String[] tokens = {/* service_account */ null, /* access_token */ null, /* secret_token */ null};

        try {
            Transaction.execute(dataSource, conn -> {
                try (var st = conn.prepareStatement("""
                        select service_account, access_token, secret_token
                        from yc_s3_credentials
                        where user_id = ?
                        for update""")) {
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
                        values (?, ?, ?, ?)""")) {
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

            response.onError(Status.INTERNAL
                .withDescription("SQL error: " + e.getMessage()).withCause(e).asException());
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

            response.onError(Status.INTERNAL
                .withDescription("S3 internal error: " + e.getMessage()).withCause(e).asException());
            return;
        }

        response.onNext(CreateS3BucketResponse.newBuilder()
            .setAmazon(LMS3.AmazonS3Endpoint.newBuilder()
                .setEndpoint(s3Creds.getEndpoint())
                .setAccessToken(tokens[1])
                .setSecretToken(tokens[2])
                .build())
            .build());
        response.onCompleted();
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
                                       StreamObserver<GetS3BucketCredentialsResponse> response) {
        LOG.debug("YandexCloudS3Storage::getBucketCredentials, userId={}, bucket={}",
            request.getUserId(), request.getBucket());

        try (var conn = dataSource.connect();
             var st = conn.prepareStatement("""
                select access_token, secret_token
                from yc_s3_credentials
                where user_id = ?""")) {
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
            throws IOException, InterruptedException {
        CloseableHttpClient httpclient = HttpClients.createDefault();

        String serviceAccountId = YcIamClient.createServiceAccount(userId, RenewableToken.getToken(), httpclient,
            ycCreds.getFolderId(), bucket);

        AWSCredentials credentials = YcIamClient.createStaticCredentials(serviceAccountId,
            RenewableToken.getToken(), httpclient);

        return new String[]{serviceAccountId, credentials.getAWSAccessKeyId(), credentials.getAWSSecretKey()};
    }
}
