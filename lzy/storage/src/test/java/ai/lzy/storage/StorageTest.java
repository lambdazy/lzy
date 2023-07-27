package ai.lzy.storage;

import ai.lzy.test.IdempotencyUtils;
import ai.lzy.test.TimeUtils;
import ai.lzy.util.auth.credentials.JwtUtils;
import ai.lzy.util.grpc.ClientHeaderInterceptor;
import ai.lzy.util.grpc.GrpcHeaders;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc.LongRunningServiceBlockingStub;
import ai.lzy.v1.storage.LSS;
import ai.lzy.v1.storage.LzyStorageServiceGrpc.LzyStorageServiceBlockingStub;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static ai.lzy.longrunning.OperationGrpcServiceUtils.awaitOperationDone;
import static ai.lzy.storage.App.APP;
import static ai.lzy.test.IdempotencyUtils.processIdempotentCallsConcurrently;
import static ai.lzy.test.IdempotencyUtils.processIdempotentCallsSequentially;
import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.v1.longrunning.LongRunningServiceGrpc.newBlockingStub;
import static org.junit.Assert.*;

public class StorageTest extends IamOnlyStorageContextTests {
    private static final int DEFAULT_TIMEOUT_SEC = 300;

    private LzyStorageServiceBlockingStub authorizedStorageClient;
    private LongRunningServiceBlockingStub opClient;

    @Before
    public void before() throws IOException {
        authorizedStorageClient = storageClient.withInterceptors(
            ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION, () -> internalUserCredentials.get().token()));
        opClient = newBlockingClient(newBlockingStub(authorizedStorageClient.getChannel()), APP,
            () -> internalUserCredentials.get().token());
    }

    @Test
    public void testUnauthenticated() {
        try {
            storageClient.createStorage(LSS.CreateStorageRequest.newBuilder()
                .setUserId("test-user")
                .setBucket("bucket-1")
                .build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.UNAUTHENTICATED.getCode(), e.getStatus().getCode());
        }

        try {
            storageClient.getStorageCredentials(LSS.GetStorageCredentialsRequest.newBuilder()
                .setUserId("test-user")
                .setBucket("bucket-1")
                .build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.UNAUTHENTICATED.getCode(), e.getStatus().getCode());
        }

        try {
            storageClient.deleteStorage(LSS.DeleteStorageRequest.newBuilder()
                .setBucket("bucket-1")
                .build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.UNAUTHENTICATED.getCode(), e.getStatus().getCode());
        }
    }

    @Test
    public void testPermissionDenied() {
        var credentials = JwtUtils.invalidCredentials("lzy-internal-user", "GITHUB");

        var client = storageClient.withInterceptors(
            ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION, credentials::token));

        try {
            client.createStorage(LSS.CreateStorageRequest.newBuilder()
                .setUserId("test-user")
                .setBucket("bucket-1")
                .build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.PERMISSION_DENIED.getCode(), e.getStatus().getCode());
        }

        try {
            client.getStorageCredentials(LSS.GetStorageCredentialsRequest.newBuilder()
                .setUserId("test-user")
                .setBucket("bucket-1")
                .build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.PERMISSION_DENIED.getCode(), e.getStatus().getCode());
        }

        try {
            client.deleteStorage(LSS.DeleteStorageRequest.newBuilder()
                .setBucket("bucket-1")
                .build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.PERMISSION_DENIED.getCode(), e.getStatus().getCode());
        }
    }

    @Test
    public void testSuccess() throws IOException {
        var respOp = authorizedStorageClient.createStorage(LSS.CreateStorageRequest.newBuilder()
            .setUserId("test-user")
            .setBucket("bucket-1")
            .build());

        TimeUtils.waitFlagUp(() -> opClient.get(LongRunning.GetOperationRequest.newBuilder()
                .setOperationId(respOp.getId()).build()).getDone(),
            DEFAULT_TIMEOUT_SEC, TimeUnit.SECONDS);

        var resp = opClient.get(LongRunning.GetOperationRequest.newBuilder()
            .setOperationId(respOp.getId()).build()).getResponse().unpack(LSS.CreateStorageResponse.class);

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

        var credsResp = authorizedStorageClient
            .getStorageCredentials(LSS.GetStorageCredentialsRequest.newBuilder()
                .setUserId("test-user")
                .setBucket("bucket-1")
                .build());
        assertTrue(credsResp.hasAmazon());
        Assert.assertEquals(resp.getS3(), credsResp.getAmazon());

        authorizedStorageClient.deleteStorage(LSS.DeleteStorageRequest.newBuilder()
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
        processIdempotentCallsSequentially(createBucketScenario());
    }

    @Test
    public void idempotentCreateBucketConcurrent() throws InterruptedException {
        processIdempotentCallsConcurrently(createBucketScenario());
    }

    private IdempotencyUtils.TestScenario<LzyStorageServiceBlockingStub, Void, LongRunning.Operation> createBucketScenario() {
        return new IdempotencyUtils.TestScenario<>(authorizedStorageClient,
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
                    assertTrue(op.getResponse().unpack(LSS.CreateStorageResponse.class).hasS3());
                } catch (InvalidProtocolBufferException e) {
                    fail(e.getMessage());
                }
            });
    }

    private static LongRunning.Operation createBucket(LzyStorageServiceBlockingStub client) {
        var userId = "some-valid-user-id";
        var bucketName = "tmp-bucket-" + userId;

        return client.createStorage(LSS.CreateStorageRequest.newBuilder()
            .setUserId(userId).setBucket(bucketName).build());
    }
}
