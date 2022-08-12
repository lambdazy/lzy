package ai.lzy.storage;

import ai.lzy.model.grpc.ChannelBuilder;
import ai.lzy.model.grpc.ClientHeaderInterceptor;
import ai.lzy.model.grpc.GrpcHeaders;
import ai.lzy.iam.test.BaseTestWithIam;
import ai.lzy.util.auth.credentials.JwtUtils;
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
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings({"UnstableApiUsage", "ResultOfMethodCallIgnored"})
public class StorageTest extends BaseTestWithIam {

    private ApplicationContext storageCtx;
    private StorageConfig storageConfig;
    private LzyStorage storageApp;

    private LzyStorageGrpc.LzyStorageBlockingStub storageClient;

    @Before
    public void before() throws IOException {
        super.before();
        storageCtx = ApplicationContext.run("../storage/src/main/resources/application-test.yml");
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
        super.after();
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
        var credentials = JwtUtils.invalidCredentials(storageConfig.iam().internal().userName());

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
        var credentials = JwtUtils.credentials(storageConfig.iam().internal().userName(),
            storageConfig.iam().internal().credentialPrivateKey());

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
}
