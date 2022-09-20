package ai.lzy.storage;

import ai.lzy.iam.test.BaseTestWithIam;
import ai.lzy.model.db.test.DatabaseTestUtils;
import ai.lzy.util.auth.credentials.JwtUtils;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.util.grpc.ClientHeaderInterceptor;
import ai.lzy.util.grpc.GrpcHeaders;
import ai.lzy.v1.storage.LSS;
import ai.lzy.v1.storage.LzyStorageServiceGrpc;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.google.common.net.HostAndPort;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.micronaut.context.ApplicationContext;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

@SuppressWarnings({"UnstableApiUsage", "ResultOfMethodCallIgnored"})
public class StorageTest extends BaseTestWithIam {

    @Rule
    public PreparedDbRule iamDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});
    @Rule
    public PreparedDbRule db = EmbeddedPostgresRules.preparedDatabase(ds -> {});

    private ApplicationContext storageCtx;
    private StorageConfig storageConfig;
    private LzyStorage storageApp;

    private LzyStorageServiceGrpc.LzyStorageServiceBlockingStub storageClient;

    @Before
    public void before() throws IOException {
        super.setUp(DatabaseTestUtils.preparePostgresConfig("iam", iamDb.getConnectionInfo()));

        storageCtx = ApplicationContext.run(DatabaseTestUtils.preparePostgresConfig("storage", db.getConnectionInfo()));
        storageConfig = storageCtx.getBean(StorageConfig.class);
        storageApp = new LzyStorage(storageCtx);
        storageApp.start();

        var channel = ChannelBuilder
            .forAddress(HostAndPort.fromString(storageConfig.getAddress()))
            .usePlaintext()
            .build();
        storageClient = LzyStorageServiceGrpc.newBlockingStub(channel);
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
        super.after();
    }

    @Test
    public void testUnauthenticated() {
        try {
            storageClient.createS3Bucket(LSS.CreateS3BucketRequest.newBuilder()
                .setUserId("test-user")
                .setBucket("bucket-1")
                .build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.UNAUTHENTICATED.getCode(), e.getStatus().getCode());
        }

        try {
            storageClient.getS3BucketCredentials(LSS.GetS3BucketCredentialsRequest.newBuilder()
                .setUserId("test-user")
                .setBucket("bucket-1")
                .build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.UNAUTHENTICATED.getCode(), e.getStatus().getCode());
        }

        try {
            storageClient.deleteS3Bucket(LSS.DeleteS3BucketRequest.newBuilder()
                .setBucket("bucket-1")
                .build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.UNAUTHENTICATED.getCode(), e.getStatus().getCode());
        }
    }

    @Test
    public void testPermissionDenied() {
        var credentials = JwtUtils.invalidCredentials(storageConfig.getIam().getInternalUserName(), "GITHUB");

        var client = storageClient.withInterceptors(
            ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION, credentials::token));

        try {
            client.createS3Bucket(LSS.CreateS3BucketRequest.newBuilder()
                .setUserId("test-user")
                .setBucket("bucket-1")
                .build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.PERMISSION_DENIED.getCode(), e.getStatus().getCode());
        }

        try {
            client.getS3BucketCredentials(LSS.GetS3BucketCredentialsRequest.newBuilder()
                .setUserId("test-user")
                .setBucket("bucket-1")
                .build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.PERMISSION_DENIED.getCode(), e.getStatus().getCode());
        }

        try {
            client.deleteS3Bucket(LSS.DeleteS3BucketRequest.newBuilder()
                .setBucket("bucket-1")
                .build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.PERMISSION_DENIED.getCode(), e.getStatus().getCode());
        }
    }

    @Test
    public void testSuccess() throws IOException {
        var credentials = storageConfig.getIam().createCredentials();

        var client = storageClient.withInterceptors(
            ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION, credentials::token));

        var resp = client.createS3Bucket(LSS.CreateS3BucketRequest.newBuilder()
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

        var credsResp = client.getS3BucketCredentials(LSS.GetS3BucketCredentialsRequest.newBuilder()
            .setUserId("test-user")
            .setBucket("bucket-1")
            .build());
        Assert.assertTrue(credsResp.hasAmazon());
        Assert.assertEquals(resp.getAmazon(), credsResp.getAmazon());

        client.deleteS3Bucket(LSS.DeleteS3BucketRequest.newBuilder()
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
}
