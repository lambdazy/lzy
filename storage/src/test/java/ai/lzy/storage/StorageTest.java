package ai.lzy.storage;

import ai.lzy.iam.LzyIAM;
import ai.lzy.iam.authorization.credentials.JwtCredentials;
import ai.lzy.model.grpc.ChannelBuilder;
import ai.lzy.model.grpc.ClientHeaderInterceptor;
import ai.lzy.model.grpc.GrpcHeaders;
import ai.lzy.v1.LzyStorageApi;
import ai.lzy.v1.LzyStorageGrpc;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.google.common.net.HostAndPort;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.PropertySource;
import io.micronaut.context.env.yaml.YamlPropertySourceLoader;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;

import static ai.lzy.model.utils.JwtCredentials.buildJWT;

@SuppressWarnings("UnstableApiUsage")
public class StorageTest {

    private ApplicationContext iamCtx;
    private LzyIAM iamApp;

    private ApplicationContext storageCtx;
    private StorageConfig storageConfig;
    private LzyStorage storageApp;

    private LzyStorageGrpc.LzyStorageBlockingStub storageClient;

    @Before
    public void before() throws IOException {
        var iamProps = new YamlPropertySourceLoader()
            .read("iam", new FileInputStream("../iam/src/main/resources/application-test.yml"));
        iamCtx = ApplicationContext.run(PropertySource.of(iamProps));
        iamApp = new LzyIAM(iamCtx);
        iamApp.start();

        storageCtx = ApplicationContext.run();
        storageConfig = storageCtx.getBean(StorageConfig.class);
        storageApp = new LzyStorage(storageCtx);
        storageApp.start();

        var channel = ChannelBuilder
            .forAddress(HostAndPort.fromString(storageConfig.address()))
            .usePlaintext()
            .build();
        storageClient = LzyStorageGrpc.newBlockingStub(channel);
    }

    @After
    public void after() {
        storageApp.close(/* force */ false);
        try {
            storageApp.awaitTermination();
        } catch (InterruptedException e) {
            // ignored
        }
        storageCtx.close();

        iamApp.close();
        iamCtx.close();
    }

    @Test
    public void testUnauthenticated() {
        try {
            storageClient.createS3Bucket(LzyStorageApi.CreateS3BucketRequest.newBuilder()
                .setUserId("test-user")
                .setBucket("bucket-1")
                .build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.UNAUTHENTICATED.getCode(), e.getStatus().getCode());
        }

        try {
            storageClient.getS3BucketCredentials(LzyStorageApi.GetS3BucketCredentialsRequest.newBuilder()
                .setUserId("test-user")
                .setBucket("bucket-1")
                .build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.UNAUTHENTICATED.getCode(), e.getStatus().getCode());
        }

        try {
            storageClient.deleteS3Bucket(LzyStorageApi.DeleteS3BucketRequest.newBuilder()
                .setBucket("bucket-1")
                .build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.UNAUTHENTICATED.getCode(), e.getStatus().getCode());
        }
    }

    @Test
    public void testPermissionDenied() {
        var credentials = internalUserCredentials(/* valid */ false);

        var client = storageClient.withInterceptors(
            ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION, credentials::token));

        try {
            client.createS3Bucket(LzyStorageApi.CreateS3BucketRequest.newBuilder()
                .setUserId("test-user")
                .setBucket("bucket-1")
                .build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.PERMISSION_DENIED.getCode(), e.getStatus().getCode());
        }

        try {
            client.getS3BucketCredentials(LzyStorageApi.GetS3BucketCredentialsRequest.newBuilder()
                .setUserId("test-user")
                .setBucket("bucket-1")
                .build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.PERMISSION_DENIED.getCode(), e.getStatus().getCode());
        }

        try {
            client.deleteS3Bucket(LzyStorageApi.DeleteS3BucketRequest.newBuilder()
                .setBucket("bucket-1")
                .build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.PERMISSION_DENIED.getCode(), e.getStatus().getCode());
        }
    }

    @Test
    public void testSuccess() throws IOException {
        var credentials = internalUserCredentials(/* valid */ true);

        var client = storageClient.withInterceptors(
            ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION, credentials::token));

        var resp = client.createS3Bucket(LzyStorageApi.CreateS3BucketRequest.newBuilder()
            .setUserId("test-user")
            .setBucket("bucket-1")
            .build());
        Assert.assertTrue(resp.toString(), resp.hasAmazon());
        Assert.assertTrue(resp.toString(), resp.getAmazon().getAccessToken().isEmpty());
        Assert.assertTrue(resp.toString(), resp.getAmazon().getSecretToken().isEmpty());

        var s3 = AmazonS3ClientBuilder.standard()
            .withPathStyleAccessEnabled(true)
            .withEndpointConfiguration(
                new AwsClientBuilder.EndpointConfiguration(resp.getAmazon().getEndpoint(), "us-west-1"))
            .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
            .build();

        s3.putObject("bucket-1", "key", "content");

        var obj = s3.getObject("bucket-1", "key");
        var content = new String(obj.getObjectContent().readAllBytes(), StandardCharsets.UTF_8);
        Assert.assertEquals("content", content);

        var credsResp = client.getS3BucketCredentials(LzyStorageApi.GetS3BucketCredentialsRequest.newBuilder()
            .setUserId("test-user")
            .setBucket("bucket-1")
            .build());
        Assert.assertTrue(credsResp.hasAmazon());
        Assert.assertEquals(resp.getAmazon(), credsResp.getAmazon());

        client.deleteS3Bucket(LzyStorageApi.DeleteS3BucketRequest.newBuilder()
            .setBucket("bucket-1")
            .build());

        try {
            s3.getObject("bucket-1", "key");
            Assert.fail();
        } catch (AmazonS3Exception e) {
            Assert.assertEquals(404, e.getStatusCode());
            Assert.assertEquals("NoSuchBucket", e.getErrorCode());
        }
    }

    public JwtCredentials internalUserCredentials(boolean valid) {
        var privateKey = valid
            ? storageConfig.iam().internal().credentialPrivateKey()
            : """
                -----BEGIN RSA PRIVATE KEY-----
                MIIEowIBAAKCAQEAn5w8xQDuTg9cc7sP3kcIH+ynzEIBSGc6JFhuOd5r82GL7F+i
                30qdaH8U5yWZR1yv9VD47sVj1zXoz/kZrHiRTczpDG3zA2EvNTNhaCD7MoYc/aqy
                hqQMr9UnS8NVn1JaZfQZJWD9rSIAKlQIFrrSaE9A6K3bdj6JXSNR8A9U9KaAg5zJ
                IgcBuBP0llUSwG0XjOMZyQrTNQmtDkvoqjVR+x5cHmwui+Y0ICJUrzOiXq8ukDHM
                1SCvWu00+c8y2tG2ce1V8eA0EFhk/pMV/q9/JKarU3Xx8wGlrArYePognSdyRPil
                yElr/QoSio+rwAuOwxnuNxB5s0CHLW8qnqhV6wIDAQABAoIBAGKI5q9MWtIQA6hi
                xHIZ8fcbd5/O49HaAHftq+bH3Gb9Qo+jnv4wpyqawcHNYWo/21UcLwHhFDkJS/gQ
                tXvXVwTryrfkrNDaT3WNicXqDonrZ7xmhB5A6qAmfEL2jUZ1Zd9pKZj83r7ira10
                ASZfIYRJ4S2EH2dJRi4cnvoPzQfXRQft4R580Y9oMt2L2dlL7R3n99yMVoNVLRZq
                Nf52XW0UUsLbbZebn6/BLh9MFhzUDroN3+IpP5vwOa26FL9NhsxUh/8WzU183VLI
                KoJNEh5opbQ0s/oNnplsU2RCYPMtrs1YYS0NAOIjG6GQY6oh1CcBTx2ebtkFFAE4
                Q0m9sDkCgYEAzHkCxxDgbY1ViR2G9KvujC3bnG7uIDb24h+jZWatx78QpcIqp3yG
                I4ItfiGCmfFW89cX1NVooOuyRcpOrLw30PrRf0V8E/icTvFkdv3LtaZmsbTtRZbw
                osB7kuoYgemhvgu67ytg8scMqooSWLKNBj/wuwCEdwdRctkhAgG9vWcCgYEAx9UN
                rcim3cqEsxl/VsXG0dJLep2EPYHs88WXcfd9q4HttZs0vLDZVXglDbmvQJiPRjGk
                C6bMnZPy+a06ly6dMtgHmF0XVwgZQhlFSlIuM23/DiivAOOJ2qo+ES6+jvsBUojo
                euRrTi8JPwv5QFU+aN6/PaJ+vsnZrHaZHBdNDN0CgYAVQZs9UI7UNLYoq+4kr178
                KaRD7fBJXw1pUnqtBvCX7E/xu26tvK9BL75E93zZPhKZBMpQcOMQn5AH21E0edif
                nAN9ZJ7SgKzXNBcKm7W6q5LPdIyaCGf5s2LlUfq8Pqp21EdZp7vLYU/6xqHDoMQy
                WyFOf25F5XfdJZ9d0wqDjwKBgQCXpxCiekxotXDPmuIQsDeatMWjYDcjlp6EwceV
                LgWpSwljcU4shOnq+yrjp69gjmbtFm8wiH1weP9EjDqS0UVreJcLAlrcKcFBcHwt
                UwDM9wVBcY6eVhAgamKAF8F2MPdn84669O6afwe9WRDnycl7PNBVriQSFo2jXL4F
                m4lV4QKBgFTIYgUi4Td8rcMJY/mduO8E7YCjj/GxQprQhS0AYlEeqdG3SZnf4dge
                gQ6GM/yke4FNfQEkkKSCVYphEReXnUJ9Bp/7cRGwB8FqHUYxN3UaP9bFg9KYWpzV
                1P75ytKqMe+x5blIRYgvizXtoNrTPVoNBIqzWa7Lt1u/Yr7ZyioC
                -----END RSA PRIVATE KEY-----""";

        try (final Reader reader = new StringReader(privateKey)) {
            return new JwtCredentials(buildJWT(storageConfig.iam().internal().userName(), reader));
        } catch (IOException | NoSuchAlgorithmException | InvalidKeySpecException e) {
            throw new RuntimeException("Cannot build credentials: " + e.getMessage(), e);
        }
    }
}
