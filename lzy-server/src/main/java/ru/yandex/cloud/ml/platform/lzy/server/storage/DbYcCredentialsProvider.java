package ru.yandex.cloud.ml.platform.lzy.server.storage;

import static ru.yandex.cloud.ml.platform.lzy.server.utils.yc.YcIamClient.createServiceAccount;
import static ru.yandex.cloud.ml.platform.lzy.server.utils.yc.YcIamClient.createStaticCredentials;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.hibernate.Session;
import org.hibernate.Transaction;
import ru.yandex.cloud.ml.platform.lzy.model.StorageCredentials;
import ru.yandex.cloud.ml.platform.lzy.server.configs.ServerConfig;
import ru.yandex.cloud.ml.platform.lzy.server.configs.StorageConfigs;
import ru.yandex.cloud.ml.platform.lzy.server.hibernate.DbStorage;
import ru.yandex.cloud.ml.platform.lzy.server.hibernate.models.UserModel;
import ru.yandex.cloud.ml.platform.lzy.server.utils.yc.RenewableToken;

@Singleton
@Requires(property = "database.enabled", value = "true")
@Requires(property = "storage.amazon.enabled", value = "true")
@Requires(property = "server.yc.enabled", value = "true")
public class DbYcCredentialsProvider implements StorageCredentialsProvider {

    @Inject
    StorageConfigs storageConfigs;

    @Inject
    DbStorage storage;

    @Inject
    ServerConfig serverConfig;

    @Override
    public StorageCredentials storageCredentials(String uid, String bucket) {
        return new AmazonCredentialsImpl(
            storageConfigs.getAmazon().getEndpoint(),
            storageConfigs.getAmazon().getAccessToken(),
            storageConfigs.getAmazon().getSecretToken()
        );
    }

    @Override
    public StorageCredentials separatedStorageCredentials(String uid, String bucket) {
        try (Session session = storage.getSessionFactory().openSession()) {
            Transaction tx = session.beginTransaction();
            UserModel user = session.find(UserModel.class, uid);
            if (user.getAccessKey() == null || user.getSecretKey() == null){
                try {
                    CloseableHttpClient httpclient = HttpClients.createDefault();
                    String serviceAccountId = createServiceAccount(
                        user.getUserId(),
                        RenewableToken.getToken(),
                        httpclient,
                        serverConfig.getYc().getFolderId(),
                        bucket
                    );
                    AWSCredentials credentials = createStaticCredentials(
                        serviceAccountId,
                        RenewableToken.getToken(),
                        httpclient
                    );
                    user.setAccessKey(credentials.getAWSAccessKeyId());
                    user.setSecretKey(credentials.getAWSSecretKey());
                    user.setServiceAccountId(serviceAccountId);
                    session.save(user);
                    tx.commit();
                }
                catch (Exception e){
                    tx.rollback();
                    throw new RuntimeException(e);
                }
            }
            AmazonS3 client = AmazonS3Client.builder().withCredentials(
                    new AWSStaticCredentialsProvider(
                            new BasicAWSCredentials(
                                    storageConfigs.getAmazon().getAccessToken(),
                                    storageConfigs.getAmazon().getSecretToken()
                            )
                    )
            ).withEndpointConfiguration(
                    new AwsClientBuilder.EndpointConfiguration(storageConfigs.getAmazon().getEndpoint(), "us-west-1")
            ).build();

            if (!client.doesBucketExistV2(bucket)){
                client.createBucket(bucket);
            }
            AccessControlList acl = client.getBucketAcl(bucket);
            Grantee grantee = new CanonicalGrantee(user.getServiceAccountId());
            acl.grantPermission(grantee, Permission.FullControl);
            client.setBucketAcl(bucket, acl);

            return new AmazonCredentialsImpl(
                storageConfigs.getAmazon().getEndpoint(),
                user.getAccessKey(),
                user.getSecretKey()
            );
        }
    }
}
