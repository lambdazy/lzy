package ai.lzy.service.storage;

import ai.lzy.longrunning.Operation;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.DbOperation;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.service.config.LzyServiceConfig;
import ai.lzy.service.dao.impl.LzyServiceStorage;
import ai.lzy.util.grpc.GrpcUtils;
import ai.lzy.v1.common.LMST;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CanonicalGrantee;
import com.amazonaws.services.s3.model.Permission;
import com.google.protobuf.Any;
import io.grpc.Status;
import io.micronaut.context.annotation.Requires;
import jakarta.annotation.Nullable;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import yandex.cloud.api.iam.v1.ServiceAccountOuterClass.ServiceAccount;
import yandex.cloud.api.iam.v1.ServiceAccountServiceGrpc;
import yandex.cloud.api.iam.v1.ServiceAccountServiceGrpc.ServiceAccountServiceBlockingStub;
import yandex.cloud.api.iam.v1.ServiceAccountServiceOuterClass.CreateServiceAccountRequest;
import yandex.cloud.api.iam.v1.awscompatibility.AccessKeyServiceGrpc;
import yandex.cloud.api.iam.v1.awscompatibility.AccessKeyServiceGrpc.AccessKeyServiceBlockingStub;
import yandex.cloud.api.iam.v1.awscompatibility.AccessKeyServiceOuterClass;
import yandex.cloud.api.operation.OperationServiceGrpc;
import yandex.cloud.api.operation.OperationServiceGrpc.OperationServiceBlockingStub;
import yandex.cloud.sdk.ServiceFactory;
import yandex.cloud.sdk.auth.Auth;
import yandex.cloud.sdk.utils.OperationUtils;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.UUID;

import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.util.grpc.ProtoConverter.toProto;

@Singleton
@Requires(property = "lzy-service.storage.yc.enabled", value = "true")
@Requires(property = "lzy-service.storage.s3.yc.enabled", value = "true")
public class YandexCloudS3Storage implements StorageService {
    private static final Duration YC_CALL_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration SA_TIMEOUT = Duration.ofSeconds(30);
    private static final Logger LOG = LogManager.getLogger(YandexCloudS3Storage.class);

    private final LzyServiceConfig.StorageConfig.S3Credentials.YcS3Credentials s3Creds;
    private final LzyServiceConfig.StorageConfig.YcCredentials ycCreds;
    private final LzyServiceStorage dataSource;

    private final OperationDao operationDao;
    private final ServiceAccountServiceBlockingStub saClient;
    private final OperationServiceBlockingStub operationService;
    private final AccessKeyServiceBlockingStub keyService;

    public YandexCloudS3Storage(LzyServiceConfig.StorageConfig.S3Credentials.YcS3Credentials s3,
                                LzyServiceConfig.StorageConfig.YcCredentials yc, LzyServiceStorage dataSource,
                                @Named("LzyServiceOperationDao") OperationDao operationDao)
    {
        this.s3Creds = s3;
        this.ycCreds = yc;
        this.dataSource = dataSource;
        this.operationDao = operationDao;

        var provider = Auth.computeEngineBuilder().build();

        var factory = ServiceFactory.builder()
            .credentialProvider(provider)
            .endpoint(yc.getEndpoint())
            .requestTimeout(YC_CALL_TIMEOUT)
            .build();

        this.saClient = factory.create(ServiceAccountServiceBlockingStub.class,
            ServiceAccountServiceGrpc::newBlockingStub);
        this.operationService = factory.create(OperationServiceBlockingStub.class,
            OperationServiceGrpc::newBlockingStub);
        this.keyService = factory.create(AccessKeyServiceBlockingStub.class,
            AccessKeyServiceGrpc::newBlockingStub);
    }

    @Override
    public void processCreateStorageOperation(String userId, String bucket, Operation operation) {
        LOG.debug("YandexCloudS3Storage::createBucket, userId={}, bucket={}", userId, bucket);

        var client = s3Client();

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

        final CreatedServiceAccount sa;
        String keyName = userId + bucket;

        if (operation.idempotencyKey() != null) {
            keyName += operation.idempotencyKey().token();
        }

        var idempotencyKey = UUID.nameUUIDFromBytes(keyName.getBytes());

        try {
            sa = withRetries(LOG, () -> getOrCreateSa(userId, bucket, idempotencyKey));
        } catch (Exception e) {
            LOG.error("SQL error while creating bucket '{}' for '{}': {}", bucket, userId, e.getMessage(), e);
            safeDeleteBucket(userId, bucket, client);

            var errorStatus = Status.INTERNAL.withDescription("SQL error: " + e.getMessage()).withCause(e);

            try {
                operationDao.failOperation(operation.id(), toProto(errorStatus), null, LOG);
                operation.completeWith(errorStatus);
            } catch (SQLException ex) {
                LOG.error("Cannot fail operation {}: {}", operation.id(), ex.getMessage());
            }

            return;
        }

        try {
            var acl = client.getBucketAcl(bucket);
            var grantee = new CanonicalGrantee(/* service_account */ sa.id);
            acl.grantPermission(grantee, Permission.FullControl);
            client.setBucketAcl(bucket, acl);
        } catch (SdkClientException e) {
            LOG.error("AWS SDK error while creating ACL at bucket '{}' for user '{}': {}. Delete bucket.",
                bucket, userId, e.getMessage(), e);
            safeDeleteBucket(userId, bucket, client);

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
            .setEndpoint(s3Creds.getEndpoint())
            .setAccessToken(sa.keyId)
            .setSecretToken(sa.keySecret)
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

    private CreatedServiceAccount getOrCreateSa(String userId, String bucket,
                                                UUID idempotencyKey) throws SQLException
    {
        try (var tx = TransactionHandle.create(dataSource)) {
            return DbOperation.execute(tx, dataSource, conn -> {
                try (PreparedStatement st = conn.prepareStatement("""
                    select service_account, access_token, secret_token
                    from yc_s3_credentials
                    where user_id = ?
                    for update"""))
                {
                    st.setString(1, userId);

                    var rs = st.executeQuery();
                    if (rs.next()) {
                        var saId = rs.getString("service_account");
                        var keyId = rs.getString("access_token");
                        var keySecret = rs.getString("secret_token");

                        return new CreatedServiceAccount(saId, keyId, keySecret);
                    }
                }

                CreatedServiceAccount newTokens;
                try {
                    newTokens = createServiceAccountForUser(userId, bucket, idempotencyKey);
                } catch (Exception e) {
                    throw new SQLException(e);
                }

                try (PreparedStatement st = conn.prepareStatement("""
                    insert into yc_s3_credentials (user_id, service_account, access_token, secret_token)
                    values (?, ?, ?, ?)"""))
                {
                    st.setString(1, userId);
                    st.setString(2, newTokens.id());
                    st.setString(3, newTokens.keyId());
                    st.setString(4, newTokens.keySecret());
                    st.executeUpdate();
                }

                tx.commit();
                return newTokens;
            });
        }
    }

    @Override
    public void deleteStorage(@Nullable String userId, String bucket) {
        LOG.debug("YandexCloudS3Storage::deleteBucket, bucket={}", bucket);
        safeDeleteBucket(userId, bucket, s3Client());
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

    /**
     * Creates a service account for the user and returns its id and access key.
     * This method is idempotent. It can create other keys for same idempotency key, but it is fine.
     * YC requires UUID as idempotency key, so we use it.
     */
    private CreatedServiceAccount createServiceAccountForUser(String userId, String bucket, UUID idempotencyKey)
        throws Exception
    {
        var saCli = GrpcUtils.withIdempotencyKey(saClient, idempotencyKey.toString());

        var op = saCli.create(CreateServiceAccountRequest.newBuilder()
            .setFolderId(ycCreds.getFolderId())
            .setDescription("service account for user " + userId)
            .setName(bucket)
            .build());
        var res = OperationUtils.wait(operationService, op, SA_TIMEOUT);
        if (res.hasError()) {
            LOG.error("Error while creating service account: {}", res.getError());
            throw new Exception("Error while creating service account");
        }

        var sa = res.getResponse().unpack(ServiceAccount.class);
        var saId = sa.getId();

        var key = keyService.create(AccessKeyServiceOuterClass.CreateAccessKeyRequest.newBuilder()
            .setServiceAccountId(saId)
            .setDescription("key for user" + userId)
            .build());

        return new CreatedServiceAccount(saId, key.getAccessKey().getKeyId(), key.getSecret());
    }

    private record CreatedServiceAccount(
        String id,
        String keyId,
        String keySecret
    ) {}
}
