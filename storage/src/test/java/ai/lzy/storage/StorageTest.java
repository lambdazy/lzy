package ai.lzy.storage;

import ai.lzy.iam.grpc.client.AuthenticateServiceGrpcClient;
import ai.lzy.iam.grpc.interceptors.AllowInternalUserOnlyInterceptor;
import ai.lzy.iam.grpc.interceptors.AuthServerInterceptor;
import ai.lzy.iam.test.BaseTestWithIam;
import ai.lzy.longrunning.OperationService;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.test.DatabaseTestUtils;
import ai.lzy.storage.config.StorageConfig;
import ai.lzy.test.TimeUtils;
import ai.lzy.util.auth.credentials.JwtUtils;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.util.grpc.ClientHeaderInterceptor;
import ai.lzy.util.grpc.GrpcHeaders;
import ai.lzy.v1.iam.LzyAuthenticateServiceGrpc;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import ai.lzy.v1.storage.LSS;
import ai.lzy.v1.storage.LSS.CreateS3BucketResponse;
import ai.lzy.v1.storage.LzyStorageServiceGrpc;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.google.common.net.HostAndPort;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.*;
import io.micronaut.context.ApplicationContext;
import io.micronaut.inject.qualifiers.Qualifiers;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import org.junit.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static ai.lzy.longrunning.OperationUtils.awaitOperationDone;
import static ai.lzy.storage.App.APP;
import static ai.lzy.util.grpc.GrpcUtils.*;
import static ai.lzy.v1.longrunning.LongRunningServiceGrpc.newBlockingStub;

@SuppressWarnings("ResultOfMethodCallIgnored")
public class StorageTest extends BaseTestWithIam {
    private static final int DEFAULT_TIMEOUT_SEC = 300;

    @Rule
    public PreparedDbRule iamDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});
    @Rule
    public PreparedDbRule db = EmbeddedPostgresRules.preparedDatabase(ds -> {});

    private ApplicationContext storageCtx;
    private StorageConfig storageConfig;
    private Server storageServer;

    private ManagedChannel iamChannel;

    private LzyStorageServiceGrpc.LzyStorageServiceBlockingStub unauthorizedStorageClient;
    private LzyStorageServiceGrpc.LzyStorageServiceBlockingStub authorizedStorageClient;

    private LongRunningServiceGrpc.LongRunningServiceBlockingStub opClient;

    @Before
    public void before() throws IOException {
        super.setUp(DatabaseTestUtils.preparePostgresConfig("iam", iamDb.getConnectionInfo()));

        storageCtx = ApplicationContext.run(DatabaseTestUtils.preparePostgresConfig("storage", db.getConnectionInfo()));
        storageConfig = storageCtx.getBean(StorageConfig.class);

        iamChannel = newGrpcChannel(storageConfig.getIam().getAddress(), LzyAuthenticateServiceGrpc.SERVICE_NAME);

        var authInterceptor = new AuthServerInterceptor(new AuthenticateServiceGrpcClient(APP, iamChannel));
        var internalOnly = new AllowInternalUserOnlyInterceptor(APP, iamChannel);

        var operationService = new OperationService(storageCtx.getBean(OperationDao.class,
            Qualifiers.byName("StorageOperationDao")));

        storageServer = App.createServer(HostAndPort.fromString(storageConfig.getAddress()), authInterceptor,
            ServerInterceptors.intercept(operationService, internalOnly),
            ServerInterceptors.intercept(storageCtx.getBean(StorageServiceGrpc.class), internalOnly));

        storageServer.start();

        var channel = ChannelBuilder
            .forAddress(HostAndPort.fromString(storageConfig.getAddress()))
            .usePlaintext()
            .build();

        unauthorizedStorageClient = LzyStorageServiceGrpc.newBlockingStub(channel);

        var credentials = storageConfig.getIam().createRenewableToken();
        authorizedStorageClient = unauthorizedStorageClient.withInterceptors(
            ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION, () -> credentials.get().token()));
        opClient = newBlockingClient(newBlockingStub(authorizedStorageClient.getChannel()), APP,
            () -> credentials.get().token());
    }

    @After
    public void after() {
        storageServer.shutdown();
        try {
            storageServer.awaitTermination();
        } catch (InterruptedException e) {
            // ignored
        }
        iamChannel.shutdown();
        storageCtx.close();
        super.after();
    }

    @Test
    public void testUnauthenticated() {
        try {
            unauthorizedStorageClient.createS3Bucket(LSS.CreateS3BucketRequest.newBuilder()
                .setUserId("test-user")
                .setBucket("bucket-1")
                .build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.UNAUTHENTICATED.getCode(), e.getStatus().getCode());
        }

        try {
            unauthorizedStorageClient.getS3BucketCredentials(LSS.GetS3BucketCredentialsRequest.newBuilder()
                .setUserId("test-user")
                .setBucket("bucket-1")
                .build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.UNAUTHENTICATED.getCode(), e.getStatus().getCode());
        }

        try {
            unauthorizedStorageClient.deleteS3Bucket(LSS.DeleteS3BucketRequest.newBuilder()
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

        var client = unauthorizedStorageClient.withInterceptors(
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
        var respOp = authorizedStorageClient.createS3Bucket(LSS.CreateS3BucketRequest.newBuilder()
            .setUserId("test-user")
            .setBucket("bucket-1")
            .build());

        TimeUtils.waitFlagUp(() -> opClient.get(LongRunning.GetOperationRequest.newBuilder()
                .setOperationId(respOp.getId()).build()).getDone(),
            DEFAULT_TIMEOUT_SEC, TimeUnit.SECONDS);

        var resp = opClient.get(LongRunning.GetOperationRequest.newBuilder()
            .setOperationId(respOp.getId()).build()).getResponse().unpack(CreateS3BucketResponse.class);

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

        var credsResp = authorizedStorageClient.getS3BucketCredentials(LSS.GetS3BucketCredentialsRequest.newBuilder()
            .setUserId("test-user")
            .setBucket("bucket-1")
            .build());
        Assert.assertTrue(credsResp.hasAmazon());
        Assert.assertEquals(resp.getAmazon(), credsResp.getAmazon());

        authorizedStorageClient.deleteS3Bucket(LSS.DeleteS3BucketRequest.newBuilder()
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

    @Test
    public void idempotentCreateBucket() throws InvalidProtocolBufferException {
        var userId = "some-valid-user-id";
        var bucketName = "tmp-bucket-" + userId;

        var idempotencyKey = "key-1";

        LongRunning.Operation op1 = withIdempotencyKey(authorizedStorageClient, idempotencyKey)
            .createS3Bucket(LSS.CreateS3BucketRequest.newBuilder()
                .setUserId(userId)
                .setBucket(bucketName)
                .build());

        op1 = awaitOperationDone(opClient, op1.getId(), Duration.ofSeconds(DEFAULT_TIMEOUT_SEC));

        Assert.assertTrue(op1.getDone());
        Assert.assertTrue(op1.hasResponse());
        Assert.assertFalse(op1.hasError());

        LongRunning.Operation op2 = withIdempotencyKey(authorizedStorageClient, idempotencyKey)
            .createS3Bucket(LSS.CreateS3BucketRequest.newBuilder()
                .setUserId(userId)
                .setBucket(bucketName)
                .build());

        op2 = awaitOperationDone(opClient, op2.getId(), Duration.ofSeconds(DEFAULT_TIMEOUT_SEC));

        Assert.assertTrue(op2.getDone());
        Assert.assertTrue(op2.hasResponse());
        Assert.assertFalse(op2.hasError());

        var op1Res = op1.getResponse().unpack(CreateS3BucketResponse.class).getAmazon();
        var op2Res = op2.getResponse().unpack(CreateS3BucketResponse.class).getAmazon();

        Assert.assertEquals(op1.getId(), op2.getId());
        Assert.assertEquals(op1Res.getEndpoint(), op2Res.getEndpoint());
        Assert.assertEquals(op1Res.getAccessToken(), op2Res.getAccessToken());
        Assert.assertEquals(op1Res.getSecretToken(), op2Res.getSecretToken());
    }

    @Test
    public void idempotentCreateBucketConcurrent() throws InterruptedException {
        var userId = "some-valid-user-id";
        var bucketName = "tmp-bucket-" + userId;

        var idempotencyKey = "key-1";

        final int N = 10;
        final var readyLatch = new CountDownLatch(N);
        final var doneLatch = new CountDownLatch(N);
        final var executor = Executors.newFixedThreadPool(N);
        final var opIds = new String[N];
        final var endpoints = new String[N];
        final var failed = new AtomicBoolean(false);

        for (int i = 0; i < N; ++i) {
            final int index = i;
            executor.submit(() -> {
                try {
                    readyLatch.countDown();
                    readyLatch.await();

                    var op = withIdempotencyKey(authorizedStorageClient, idempotencyKey)
                        .createS3Bucket(LSS.CreateS3BucketRequest.newBuilder()
                            .setUserId(userId)
                            .setBucket(bucketName)
                            .build());
                    op = awaitOperationDone(opClient, op.getId(), Duration.ofSeconds(DEFAULT_TIMEOUT_SEC));
                    Assert.assertFalse(op.getId().isEmpty());
                    Assert.assertFalse(op.hasError());
                    Assert.assertTrue(op.hasResponse());

                    opIds[index] = op.getId();
                    endpoints[index] = op.getResponse().unpack(CreateS3BucketResponse.class).getAmazon().getEndpoint();
                } catch (Exception e) {
                    failed.set(true);
                    e.printStackTrace(System.err);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        doneLatch.await();
        executor.shutdown();

        Assert.assertFalse(failed.get());
        Assert.assertFalse(opIds[0].isEmpty());
        Assert.assertTrue(Arrays.stream(opIds).allMatch(opId -> opId.equals(opIds[0])));
        Assert.assertFalse(endpoints[0].isEmpty());
        Assert.assertTrue(Arrays.stream(endpoints).allMatch(endpoint -> endpoint.equals(endpoints[0])));
    }
}
