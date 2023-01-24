package ai.lzy.storage;

import ai.lzy.iam.grpc.client.AuthenticateServiceGrpcClient;
import ai.lzy.iam.grpc.interceptors.AllowInternalUserOnlyInterceptor;
import ai.lzy.iam.grpc.interceptors.AuthServerInterceptor;
import ai.lzy.iam.test.BaseTestWithIam;
import ai.lzy.longrunning.OperationsService;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.test.DatabaseTestUtils;
import ai.lzy.storage.config.StorageConfig;
import ai.lzy.test.IdempotencyUtils.TestScenario;
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
import ai.lzy.v1.storage.LzyStorageServiceGrpc.LzyStorageServiceBlockingStub;
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
import java.util.concurrent.TimeUnit;

import static ai.lzy.longrunning.OperationUtils.awaitOperationDone;
import static ai.lzy.storage.App.APP;
import static ai.lzy.test.IdempotencyUtils.processConcurrently;
import static ai.lzy.test.IdempotencyUtils.processSequentially;
import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;
import static ai.lzy.v1.longrunning.LongRunningServiceGrpc.newBlockingStub;
import static org.junit.Assert.*;

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

    private LzyStorageServiceBlockingStub unauthorizedStorageClient;
    private LzyStorageServiceBlockingStub authorizedStorageClient;

    private LongRunningServiceGrpc.LongRunningServiceBlockingStub opClient;

    @Before
    public void before() throws IOException {
        super.setUp(DatabaseTestUtils.preparePostgresConfig("iam", iamDb.getConnectionInfo()));

        storageCtx = ApplicationContext.run(DatabaseTestUtils.preparePostgresConfig("storage", db.getConnectionInfo()));
        storageConfig = storageCtx.getBean(StorageConfig.class);
        storageConfig.getIam().setAddress("localhost:" + super.getPort());

        iamChannel = newGrpcChannel(storageConfig.getIam().getAddress(), LzyAuthenticateServiceGrpc.SERVICE_NAME);

        var authInterceptor = new AuthServerInterceptor(new AuthenticateServiceGrpcClient(APP, iamChannel));
        var internalOnly = new AllowInternalUserOnlyInterceptor(APP, iamChannel);

        var operationService = new OperationsService(storageCtx.getBean(OperationDao.class,
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

        assertTrue(resp.toString(), resp.hasS3());
        assertTrue(resp.toString(), resp.getS3().getAccessToken().isEmpty());
        assertTrue(resp.toString(), resp.getS3().getSecretToken().isEmpty());

        var s3 = AmazonS3ClientBuilder.standard()
            .withPathStyleAccessEnabled(true)
            .withEndpointConfiguration(
                new AwsClientBuilder.EndpointConfiguration(resp.getS3().getEndpoint(), "us-west-1"))
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
        assertTrue(credsResp.hasAmazon());
        Assert.assertEquals(resp.getS3(), credsResp.getAmazon());

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
    public void idempotentCreateBucket() {
        processSequentially(createBucketScenario());
    }

    @Test
    public void idempotentCreateBucketConcurrent() throws InterruptedException {
        processConcurrently(createBucketScenario());
    }

    private TestScenario<LzyStorageServiceBlockingStub, Void, LongRunning.Operation> createBucketScenario() {
        return new TestScenario<>(authorizedStorageClient,
            stub -> null,
            (stub, nothing) -> {
                var op = createBucket(stub);
                return awaitOperationDone(opClient, op.getId(), Duration.ofSeconds(DEFAULT_TIMEOUT_SEC));
            },
            op -> {
                assertTrue(op.getDone());
                assertTrue(op.hasResponse());
                assertFalse(op.hasError());

                try {
                    assertTrue(op.getResponse().unpack(CreateS3BucketResponse.class).hasS3());
                } catch (InvalidProtocolBufferException e) {
                    fail(e.getMessage());
                }
            });
    }

    private static LongRunning.Operation createBucket(LzyStorageServiceBlockingStub client) {
        var userId = "some-valid-user-id";
        var bucketName = "tmp-bucket-" + userId;

        return client.createS3Bucket(LSS.CreateS3BucketRequest.newBuilder()
            .setUserId(userId).setBucket(bucketName).build());
    }
}
