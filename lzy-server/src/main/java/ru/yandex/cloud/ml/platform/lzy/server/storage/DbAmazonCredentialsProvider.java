package ru.yandex.cloud.ml.platform.lzy.server.storage;

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
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemReader;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.json.JSONObject;
import ru.yandex.cloud.ml.platform.lzy.server.configs.ServerConfig;
import ru.yandex.cloud.ml.platform.lzy.server.configs.StorageConfigs;
import ru.yandex.cloud.ml.platform.lzy.server.hibernate.DbStorage;
import ru.yandex.cloud.ml.platform.lzy.server.hibernate.models.UserModel;
import ru.yandex.cloud.ml.platform.lzy.server.utils.RenewableToken;
import ru.yandex.cloud.ml.platform.lzy.server.utils.TokenSupplier;
import ru.yandex.cloud.ml.platform.lzy.server.utils.YcOperation;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy;

import java.io.IOException;
import java.io.StringReader;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;

import static ru.yandex.cloud.ml.platform.lzy.server.utils.StorageUtils.getCredentialsByBucket;

@Singleton
@Requires(property = "database.enabled", value = "true")
@Requires(property = "storage.amazon.enabled", value = "true")
@Requires(property = "server.yc.enabled", value = "true")
public class DbAmazonCredentialsProvider implements StorageCredentialsProvider {
    private static final Logger LOG = LogManager.getLogger(DbAmazonCredentialsProvider.class);

    @Inject
    StorageConfigs storageConfigs;

    @Inject
    DbStorage storage;

    @Inject
    ServerConfig serverConfig;

    private final RenewableToken token;

    public DbAmazonCredentialsProvider(ServerConfig serverConfig) {
        PemObject privateKeyPem;
        LOG.info(
                String.format("folderId: %s\n saId: %s", serverConfig.getYc().getFolderId(), serverConfig.getYc().getServiceAccountId())
        );
        try (PemReader reader = new PemReader(new StringReader(serverConfig.getYc().getPrivateKey()))) {
            privateKeyPem = reader.readPemObject();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        PrivateKey privateKey;
        try {
            KeyFactory keyFactory = KeyFactory.getInstance("RSA");
            privateKey = keyFactory.generatePrivate(new PKCS8EncodedKeySpec(privateKeyPem.getContent()));
        } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
            throw new RuntimeException(e);
        }

        token = new RenewableToken(
            new TokenSupplier(
                serverConfig.getYc().getServiceAccountId(),
                serverConfig.getYc().getKeyId(),
                privateKey
            )
        );
    }

    @Override
    public Lzy.GetS3CredentialsResponse storageCredentials(String uid) {
        return Lzy.GetS3CredentialsResponse.newBuilder()
                .setBucket(storageConfigs.getBucket())
                .setAmazon(
                        Lzy.AmazonCredentials.newBuilder()
                                .setAccessToken(storageConfigs.getAmazon().getAccessToken())
                                .setSecretToken(storageConfigs.getAmazon().getSecretToken())
                                .setEndpoint(storageConfigs.getAmazon().getEndpoint())
                                .build()
                )
                .build();
    }

    @Override
    public Lzy.GetS3CredentialsResponse separatedStorageCredentials(String uid) {
        try (Session session = storage.getSessionFactory().openSession()) {
            Transaction tx = session.beginTransaction();
            UserModel user = session.find(UserModel.class, uid);
            if (user.getAccessKey() == null || user.getSecretKey() == null){
                try {
                    CloseableHttpClient httpclient = HttpClients.createDefault();
                    String serviceAccountId = createServiceAccount(user.getUserId(), token.get(), httpclient, serverConfig.getYc().getFolderId(), user.getBucket());
                    AWSCredentials credentials = createStaticCredentials(serviceAccountId, token.get(), httpclient);
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

            if (!client.doesBucketExistV2(user.getBucket())){
                client.createBucket(user.getBucket());
            }
            AccessControlList acl = client.getBucketAcl(user.getBucket());
            Grantee grantee = new CanonicalGrantee(user.getServiceAccountId());
            acl.grantPermission(grantee, Permission.FullControl);
            client.setBucketAcl(user.getBucket(), acl);

            return Lzy.GetS3CredentialsResponse.newBuilder()
                .setBucket(user.getBucket())
                .setAmazon(Lzy.AmazonCredentials.newBuilder()
                        .setEndpoint(storageConfigs.getAmazon().getEndpoint())
                        .setAccessToken(user.getAccessKey())
                        .setSecretToken(user.getSecretKey())
                        .build())
                .build();
        }
    }

    private String createServiceAccount(String uid, String iamToken, CloseableHttpClient httpclient, String folderId, String bucket) throws IOException, InterruptedException {
        HttpPost request = new HttpPost("https://iam.api.cloud.yandex.net/iam/v1/serviceAccounts/");
        String requestBody = new JSONObject()
                .put("folderId", folderId)
                .put("name", bucket)
                .put("description", "service account for user " + uid)
                .toString();
        String opId = executeRequest(iamToken, httpclient, request, requestBody).getString("id");
        JSONObject obj = YcOperation.getResult(opId, httpclient, iamToken);
        return obj.getString("id");
    }

    private JSONObject executeRequest(String iamToken, CloseableHttpClient httpclient, HttpPost request, String requestBody) throws IOException {
        StringEntity requestEntity = new StringEntity(requestBody);
        request.setEntity(requestEntity);
        request.setHeader("Content-type", "application/json");
        request.setHeader("Authorization", "Bearer " + iamToken);
        CloseableHttpResponse response = httpclient.execute(request);
        if (response.getStatusLine().getStatusCode() != 200){
            throw new RuntimeException(String.format("Code: %s, reason: %s", response.getStatusLine().getStatusCode(), response.getStatusLine().getReasonPhrase()));
        }
        String body = EntityUtils.toString(response.getEntity());
        return new JSONObject(body);
    }


    private AWSCredentials createStaticCredentials(String serviceAccountId, String iamToken, CloseableHttpClient httpclient) throws IOException {
        HttpPost httppost = new HttpPost("https://iam.api.cloud.yandex.net/iam/aws-compatibility/v1/accessKeys/");
        String requestBody = new JSONObject()
                .put("serviceAccountId", serviceAccountId)
                .put("description", "key for service account id " + serviceAccountId)
                .toString();
        JSONObject credentials = executeRequest(iamToken, httpclient, httppost, requestBody);

        String accessKeyId = credentials.getJSONObject("accessKey").getString("keyId");
        String secretKey = credentials.getString("secret");
        return new BasicAWSCredentials(accessKeyId, secretKey);
    }
}
