package ai.lzy.allocator.test;

import static ai.lzy.allocator.test.Utils.waitOperation;

import ai.lzy.allocator.AllocatorMain;
import ai.lzy.allocator.alloc.impl.kuber.KuberClientFactory;
import ai.lzy.allocator.alloc.impl.kuber.KuberLabels;
import ai.lzy.allocator.alloc.impl.kuber.KuberVmAllocator;
import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.dao.impl.AllocatorDataSource;
import ai.lzy.allocator.dao.impl.SessionDaoImpl;
import ai.lzy.iam.test.BaseTestWithIam;
import ai.lzy.model.db.test.DatabaseTestUtils;
import ai.lzy.test.TimeUtils;
import ai.lzy.util.auth.credentials.JwtUtils;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.util.grpc.ClientHeaderInterceptor;
import ai.lzy.util.grpc.GrpcHeaders;
import ai.lzy.util.grpc.ProtoConverter;
import ai.lzy.v1.*;
import ai.lzy.v1.OperationService.Operation;
import ai.lzy.v1.VmAllocatorApi.*;
import com.google.common.net.HostAndPort;
import com.google.protobuf.Duration;
import com.google.protobuf.InvalidProtocolBufferException;
import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.client.server.mock.KubernetesServer;
import io.fabric8.kubernetes.client.utils.Serialization;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.micronaut.context.ApplicationContext;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import org.junit.*;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;

public class AllocatorApiTest extends BaseTestWithIam {

    private static final int TIMEOUT_SEC = 300;

    @Rule
    public PreparedDbRule iamDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});
    @Rule
    public PreparedDbRule db = EmbeddedPostgresRules.preparedDatabase(ds -> {});

    private ApplicationContext allocatorCtx;
    private AllocatorGrpc.AllocatorBlockingStub unauthorizedAllocatorBlockingStub;
    private AllocatorGrpc.AllocatorBlockingStub authorizedAllocatorBlockingStub;
    private AllocatorPrivateGrpc.AllocatorPrivateBlockingStub privateAllocatorBlockingStub;
    private OperationServiceApiGrpc.OperationServiceApiBlockingStub operationServiceApiBlockingStub;
    private AllocatorMain allocatorApp;
    private KubernetesServer kubernetesServer;
    private ManagedChannel channel;

    @Before
    public void before() throws IOException {
        super.setUp(DatabaseTestUtils.preparePostgresConfig("iam", iamDb.getConnectionInfo()));

        kubernetesServer = new KubernetesServer();
        kubernetesServer.before();
        kubernetesServer.expect().post().withPath("/api/v1/pods")
            .andReturn(HttpURLConnection.HTTP_OK, new PodListBuilder().build()).always();

        final Node node = new Node();

        node.setStatus(
            new NodeStatusBuilder()
                .withAddresses(new NodeAddressBuilder()
                    .withAddress("localhost")
                    .withType("HostName")
                    .build())
                .build()
        );

        kubernetesServer.expect().get().withPath("/api/v1/nodes/node")
            .andReturn(HttpURLConnection.HTTP_OK, node)
            .always();

        var props = DatabaseTestUtils.preparePostgresConfig("allocator", db.getConnectionInfo());
        // props.putAll(DatabaseTestUtils.prepareLocalhostConfig("allocator"));

        allocatorCtx = ApplicationContext.run(props);
        ((MockKuberClientFactory) allocatorCtx.getBean(KuberClientFactory.class)).setClientSupplier(
            () -> kubernetesServer.getKubernetesMockServer().createClient()
        );

        allocatorApp = allocatorCtx.getBean(AllocatorMain.class);
        allocatorApp.start();

        final var config = allocatorCtx.getBean(ServiceConfig.class);
        //noinspection UnstableApiUsage
        channel = ChannelBuilder
            .forAddress(HostAndPort.fromString(config.getAddress()))
            .usePlaintext()
            .build();
        var credentials = config.getIam().createCredentials();
        unauthorizedAllocatorBlockingStub = AllocatorGrpc.newBlockingStub(channel);
        privateAllocatorBlockingStub = AllocatorPrivateGrpc.newBlockingStub(channel).withInterceptors(
            ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION, credentials::token));
        operationServiceApiBlockingStub = OperationServiceApiGrpc.newBlockingStub(channel).withInterceptors(
            ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION, credentials::token));
        authorizedAllocatorBlockingStub = unauthorizedAllocatorBlockingStub.withInterceptors(
            ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION, credentials::token));
    }

    @After
    public void after() {
        allocatorApp.stop();
        try {
            allocatorApp.awaitTermination();
        } catch (InterruptedException ignored) {
            // ignored
        }

        channel.shutdown();
        try {
            channel.awaitTermination(60, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
            //ignored
        }

        DatabaseTestUtils.cleanup(allocatorCtx.getBean(AllocatorDataSource.class));

        allocatorCtx.stop();
        kubernetesServer.after();
        super.after();
    }

    @Test
    public void testUnauthenticated() {
        try {
            //noinspection ResultOfMethodCallIgnored
            unauthorizedAllocatorBlockingStub.createSession(CreateSessionRequest.newBuilder().build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.UNAUTHENTICATED.getCode(), e.getStatus().getCode());
        }
        try {
            //noinspection ResultOfMethodCallIgnored
            unauthorizedAllocatorBlockingStub.deleteSession(DeleteSessionRequest.newBuilder().build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.UNAUTHENTICATED.getCode(), e.getStatus().getCode());
        }
        try {
            //noinspection ResultOfMethodCallIgnored
            unauthorizedAllocatorBlockingStub.allocate(AllocateRequest.newBuilder().build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.UNAUTHENTICATED.getCode(), e.getStatus().getCode());
        }
        try {
            //noinspection ResultOfMethodCallIgnored
            unauthorizedAllocatorBlockingStub.free(FreeRequest.newBuilder().build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.UNAUTHENTICATED.getCode(), e.getStatus().getCode());
        }
    }

    @Test
    public void testPermissionDenied() {
        final AllocatorGrpc.AllocatorBlockingStub invalidAuthorizedAllocatorBlockingStub =
            unauthorizedAllocatorBlockingStub.withInterceptors(
                ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION,
                    JwtUtils.invalidCredentials("user")::token));
        try {
            //noinspection ResultOfMethodCallIgnored
            invalidAuthorizedAllocatorBlockingStub.createSession(CreateSessionRequest.newBuilder().build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.PERMISSION_DENIED.getCode(), e.getStatus().getCode());
        }
        try {
            //noinspection ResultOfMethodCallIgnored
            invalidAuthorizedAllocatorBlockingStub.deleteSession(DeleteSessionRequest.newBuilder().build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.PERMISSION_DENIED.getCode(), e.getStatus().getCode());
        }
        try {
            //noinspection ResultOfMethodCallIgnored
            invalidAuthorizedAllocatorBlockingStub.allocate(AllocateRequest.newBuilder().build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.PERMISSION_DENIED.getCode(), e.getStatus().getCode());
        }
        try {
            //noinspection ResultOfMethodCallIgnored
            invalidAuthorizedAllocatorBlockingStub.free(FreeRequest.newBuilder().build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.PERMISSION_DENIED.getCode(), e.getStatus().getCode());
        }
    }

    @Test
    public void testCreateAndDeleteSession() {
        final CreateSessionResponse createSessionResponse = authorizedAllocatorBlockingStub.createSession(
            CreateSessionRequest.newBuilder().setOwner(UUID.randomUUID().toString()).setCachePolicy(
                    CachePolicy.newBuilder().setIdleTimeout(Duration.newBuilder().setSeconds(100).build()).build())
                .build());
        final DeleteSessionResponse deleteSessionResponse = authorizedAllocatorBlockingStub.deleteSession(
            DeleteSessionRequest.newBuilder().setSessionId(createSessionResponse.getSessionId()).build());

        Assert.assertNotNull(createSessionResponse.getSessionId());
        Assert.assertNotNull(deleteSessionResponse);
    }

    @Test
    public void createSessionNoOwner() {
        try {
            //noinspection ResultOfMethodCallIgnored
            authorizedAllocatorBlockingStub.createSession(CreateSessionRequest.newBuilder().setCachePolicy(
                    CachePolicy.newBuilder().setIdleTimeout(Duration.newBuilder().setSeconds(100).build()).build())
                .build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.INVALID_ARGUMENT.getCode(), e.getStatus().getCode());
        }
    }

    @Test
    public void createSessionNoCachePolicy() {
        try {
            //noinspection ResultOfMethodCallIgnored
            authorizedAllocatorBlockingStub.createSession(
                CreateSessionRequest.newBuilder().setOwner(UUID.randomUUID().toString()).build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.INVALID_ARGUMENT.getCode(), e.getStatus().getCode());
        }
    }

    @Test
    public void createSessionNoCachePolicyDuration() {
        try {
            //noinspection ResultOfMethodCallIgnored
            authorizedAllocatorBlockingStub.createSession(
                CreateSessionRequest.newBuilder().setOwner(UUID.randomUUID().toString()).setCachePolicy(
                    CachePolicy.newBuilder().build()).build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.INVALID_ARGUMENT.getCode(), e.getStatus().getCode());
        }
    }

    @Test
    public void errorWhileCreatingSession() {
        allocatorCtx.getBean(SessionDaoImpl.class).injectError(new SQLException("non retryable", "xxx"));

        try {
            var resp = authorizedAllocatorBlockingStub.createSession(
                CreateSessionRequest.newBuilder()
                    .setOwner(UUID.randomUUID().toString())
                    .setCachePolicy(CachePolicy.newBuilder()
                        .setIdleTimeout(ProtoConverter.toProto(java.time.Duration.ofHours(1)))
                        .build())
                    .build());
            Assert.fail(resp.getSessionId());
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(Status.Code.INTERNAL, e.getStatus().getCode());
            Assert.assertEquals("non retryable", e.getStatus().getDescription());
        }
    }

    @Test
    public void retryableSqlErrorWhileCreatingSession() {
        allocatorCtx.getBean(SessionDaoImpl.class).injectError(
            new PSQLException("retry me, plz", PSQLState.CONNECTION_FAILURE));

        var resp = authorizedAllocatorBlockingStub.createSession(
            CreateSessionRequest.newBuilder()
                .setOwner(UUID.randomUUID().toString())
                .setCachePolicy(CachePolicy.newBuilder()
                    .setIdleTimeout(ProtoConverter.toProto(java.time.Duration.ofHours(1)))
                    .build())
                .build());
        Assert.assertNotNull(resp.getSessionId());
    }

    @Test
    public void allocateKuberErrorWhileCreateTest() throws InvalidProtocolBufferException, InterruptedException {
        //simulate kuber api error on pod creation
        kubernetesServer.getKubernetesMockServer().clearExpectations();
        kubernetesServer.expect().post().withPath("/api/v1/namespaces/default/pods")
            .andReturn(HttpURLConnection.HTTP_INTERNAL_ERROR, new PodListBuilder().build())
            .once();

        final CreateSessionResponse createSessionResponse = authorizedAllocatorBlockingStub.createSession(
            CreateSessionRequest.newBuilder().setOwner(UUID.randomUUID().toString()).setCachePolicy(
                    CachePolicy.newBuilder().setIdleTimeout(Duration.newBuilder().setSeconds(100).build()).build())
                .build());
        final Operation operation = authorizedAllocatorBlockingStub.allocate(AllocateRequest.newBuilder()
            .setSessionId(createSessionResponse.getSessionId())
            .setPoolLabel("S")
            .build());
        final VmAllocatorApi.AllocateMetadata allocateMetadata =
            operation.getMetadata().unpack(VmAllocatorApi.AllocateMetadata.class);

        final String podName = KuberVmAllocator.POD_NAME_PREFIX + allocateMetadata.getVmId();
        mockGetPod(podName);
        final CountDownLatch kuberRemoveRequestLatch = new CountDownLatch(1);
        mockDeletePod(podName, kuberRemoveRequestLatch::countDown, HttpURLConnection.HTTP_OK);

        final Operation allocate = waitOp(operation);
        Assert.assertEquals(Status.INTERNAL.getCode().value(), allocate.getError().getCode());
        Assert.assertTrue(kuberRemoveRequestLatch.await(TIMEOUT_SEC, TimeUnit.SECONDS));
    }

    @Test
    public void allocateServantTimeoutTest() throws InvalidProtocolBufferException,
            InterruptedException, ExecutionException
    {
        final CreateSessionResponse createSessionResponse = authorizedAllocatorBlockingStub.createSession(
            CreateSessionRequest.newBuilder().setOwner(UUID.randomUUID().toString()).setCachePolicy(
                    CachePolicy.newBuilder().setIdleTimeout(Duration.newBuilder().setSeconds(100).build()).build())
                .build());

        final var future = awaitAllocationRequest();

        final Operation operation = authorizedAllocatorBlockingStub.allocate(AllocateRequest.newBuilder()
            .setSessionId(createSessionResponse.getSessionId())
            .setPoolLabel("S")
            .build());
        final VmAllocatorApi.AllocateMetadata allocateMetadata =
            operation.getMetadata().unpack(VmAllocatorApi.AllocateMetadata.class);

        final String podName = future.get();
        mockGetPod(podName);
        
        final CountDownLatch kuberRemoveRequestLatch = new CountDownLatch(1);
        mockDeletePod(podName, kuberRemoveRequestLatch::countDown, HttpURLConnection.HTTP_OK);

        final Operation allocate = waitOp(operation);
        Assert.assertEquals(Status.DEADLINE_EXCEEDED.getCode().value(), allocate.getError().getCode());
        Assert.assertTrue(kuberRemoveRequestLatch.await(TIMEOUT_SEC, TimeUnit.SECONDS));
    }

    @Test
    public void allocateInvalidPoolTest() {
        final CreateSessionResponse createSessionResponse = authorizedAllocatorBlockingStub.createSession(
            CreateSessionRequest.newBuilder().setOwner(UUID.randomUUID().toString()).setCachePolicy(
                    CachePolicy.newBuilder().setIdleTimeout(Duration.newBuilder().setSeconds(100).build()).build())
                .build());
        final Operation allocate = waitOp(authorizedAllocatorBlockingStub.allocate(AllocateRequest.newBuilder()
            .setSessionId(createSessionResponse.getSessionId())
            .setPoolLabel("LABEL")
            .build()));
        Assert.assertEquals(Status.INVALID_ARGUMENT.getCode().value(), allocate.getError().getCode());
    }

    @Test
    public void allocateInvalidSessionTest() {
        try {
            waitOp(authorizedAllocatorBlockingStub.allocate(AllocateRequest.newBuilder()
                .setSessionId(UUID.randomUUID().toString())
                .setPoolLabel("S")
                .build()));
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.INVALID_ARGUMENT.getCode(), e.getStatus().getCode());
        }

    }

    @Test
    public void allocateFreeSuccessTest() throws InvalidProtocolBufferException,
            InterruptedException, ExecutionException
    {
        final CreateSessionResponse createSessionResponse = authorizedAllocatorBlockingStub.createSession(
            CreateSessionRequest.newBuilder().setOwner(UUID.randomUUID().toString()).setCachePolicy(
                    CachePolicy.newBuilder().setIdleTimeout(Duration.newBuilder().setSeconds(0).build()).build())
                .build());

        final var future = awaitAllocationRequest();

        final Operation allocationStarted = authorizedAllocatorBlockingStub.allocate(AllocateRequest.newBuilder()
            .setSessionId(createSessionResponse.getSessionId())
            .setPoolLabel("S")
            .build());
        final VmAllocatorApi.AllocateMetadata allocateMetadata =
            allocationStarted.getMetadata().unpack(VmAllocatorApi.AllocateMetadata.class);

        final String podName = future.get();
        mockGetPod(podName);
        final CountDownLatch kuberRemoveRequestLatch = new CountDownLatch(1);
        mockDeletePod(podName, kuberRemoveRequestLatch::countDown, HttpURLConnection.HTTP_OK);

        registerVm(allocateMetadata.getVmId());
        final Operation allocate = waitOp(allocationStarted);
        final VmAllocatorApi.AllocateResponse allocateResponse =
            allocate.getResponse().unpack(VmAllocatorApi.AllocateResponse.class);
        //noinspection ResultOfMethodCallIgnored
        authorizedAllocatorBlockingStub.free(FreeRequest.newBuilder().setVmId(allocateMetadata.getVmId()).build());

        Assert.assertTrue(allocate.getDone());
        Assert.assertEquals(createSessionResponse.getSessionId(), allocateResponse.getSessionId());
        Assert.assertEquals("S", allocateResponse.getPoolId());
        Assert.assertTrue(kuberRemoveRequestLatch.await(TIMEOUT_SEC, TimeUnit.SECONDS));
    }

    @Test
    public void eventualFreeAfterFailTest() throws InvalidProtocolBufferException,
            InterruptedException, ExecutionException
    {
        final CreateSessionResponse createSessionResponse = authorizedAllocatorBlockingStub.createSession(
            CreateSessionRequest.newBuilder().setOwner(UUID.randomUUID().toString()).setCachePolicy(
                    CachePolicy.newBuilder().setIdleTimeout(Duration.newBuilder().setSeconds(0).build()).build())
                .build());

        final var future = awaitAllocationRequest();

        final Operation allocationStarted = authorizedAllocatorBlockingStub.allocate(AllocateRequest.newBuilder()
            .setSessionId(createSessionResponse.getSessionId())
            .setPoolLabel("S")
            .build());
        final VmAllocatorApi.AllocateMetadata allocateMetadata =
            allocationStarted.getMetadata().unpack(VmAllocatorApi.AllocateMetadata.class);

        final String podName = future.get();
        mockGetPod(podName);
        final CountDownLatch kuberRemoveRequestLatch = new CountDownLatch(2);
        mockDeletePod(podName, () -> {
            kuberRemoveRequestLatch.countDown();
            mockDeletePod(podName, kuberRemoveRequestLatch::countDown, HttpURLConnection.HTTP_OK);
        }, HttpURLConnection.HTTP_INTERNAL_ERROR);

        registerVm(allocateMetadata.getVmId());
        final Operation allocate = waitOp(allocationStarted);
        final VmAllocatorApi.AllocateResponse allocateResponse =
            allocate.getResponse().unpack(VmAllocatorApi.AllocateResponse.class);
        //noinspection ResultOfMethodCallIgnored
        authorizedAllocatorBlockingStub.free(FreeRequest.newBuilder().setVmId(allocateMetadata.getVmId()).build());

        Assert.assertTrue(allocate.getDone());
        Assert.assertEquals(createSessionResponse.getSessionId(), allocateResponse.getSessionId());
        Assert.assertEquals("S", allocateResponse.getPoolId());
        Assert.assertTrue(kuberRemoveRequestLatch.await(TIMEOUT_SEC, TimeUnit.SECONDS));
    }

    @Test
    public void allocateFromCacheTest() throws InvalidProtocolBufferException,
            InterruptedException, ExecutionException
    {
        final CreateSessionResponse createSessionResponse = authorizedAllocatorBlockingStub.createSession(
            CreateSessionRequest.newBuilder().setOwner(UUID.randomUUID().toString()).setCachePolicy(
                    CachePolicy.newBuilder().setIdleTimeout(Duration.newBuilder().setSeconds(5).build()).build())
                .build());

        final var future = awaitAllocationRequest();

        final Operation operationFirst = authorizedAllocatorBlockingStub.allocate(AllocateRequest.newBuilder()
            .setSessionId(createSessionResponse.getSessionId())
            .setPoolLabel("S")
            .build());
        final VmAllocatorApi.AllocateMetadata allocateMetadataFirst =
            operationFirst.getMetadata().unpack(VmAllocatorApi.AllocateMetadata.class);

        final String firstPodName = future.get();
        mockGetPod(firstPodName);

        registerVm(allocateMetadataFirst.getVmId());
        waitOp(operationFirst);

        //noinspection ResultOfMethodCallIgnored
        authorizedAllocatorBlockingStub.free(FreeRequest.newBuilder().setVmId(allocateMetadataFirst.getVmId()).build());

        final Operation operationSecond = authorizedAllocatorBlockingStub.allocate(AllocateRequest.newBuilder()
            .setSessionId(createSessionResponse.getSessionId())
            .setPoolLabel("S")
            .build());
        final VmAllocatorApi.AllocateMetadata allocateMetadataSecond =
            operationSecond.getMetadata().unpack(VmAllocatorApi.AllocateMetadata.class);

        final CountDownLatch kuberRemoveRequestLatch = new CountDownLatch(1);
        mockDeletePod(firstPodName, kuberRemoveRequestLatch::countDown, HttpURLConnection.HTTP_OK);
        waitOp(operationSecond);

        //noinspection ResultOfMethodCallIgnored
        authorizedAllocatorBlockingStub.free(
            FreeRequest.newBuilder().setVmId(allocateMetadataSecond.getVmId()).build());

        Assert.assertEquals(allocateMetadataFirst.getVmId(), allocateMetadataSecond.getVmId());
        Assert.assertTrue(kuberRemoveRequestLatch.await(TIMEOUT_SEC, TimeUnit.SECONDS));
    }

    @Test
    public void deleteSessionWithActiveVmsAfterRegister() throws InvalidProtocolBufferException,
            InterruptedException, ExecutionException
    {
        final CreateSessionResponse createSessionResponse = authorizedAllocatorBlockingStub.createSession(
            CreateSessionRequest.newBuilder().setOwner(UUID.randomUUID().toString()).setCachePolicy(
                    CachePolicy.newBuilder().setIdleTimeout(Duration.newBuilder().setSeconds(0).build()).build())
                .build());

        final var future = awaitAllocationRequest();

        final Operation allocate = authorizedAllocatorBlockingStub.allocate(AllocateRequest.newBuilder()
            .setSessionId(createSessionResponse.getSessionId())
            .setPoolLabel("S")
            .build());
        final VmAllocatorApi.AllocateMetadata allocateMetadata =
            allocate.getMetadata().unpack(VmAllocatorApi.AllocateMetadata.class);

        final String podName = future.get();
        mockGetPod(podName);
        final CountDownLatch kuberRemoveRequestLatch = new CountDownLatch(1);
        mockDeletePod(podName, kuberRemoveRequestLatch::countDown, HttpURLConnection.HTTP_OK);

        registerVm(allocateMetadata.getVmId());
        //noinspection ResultOfMethodCallIgnored
        authorizedAllocatorBlockingStub.deleteSession(
            DeleteSessionRequest.newBuilder().setSessionId(createSessionResponse.getSessionId()).build());

        Assert.assertTrue(kuberRemoveRequestLatch.await(TIMEOUT_SEC, TimeUnit.SECONDS));
    }

    @Test
    public void deleteSessionWithActiveVmsBeforeRegister() throws InvalidProtocolBufferException,
            InterruptedException, ExecutionException
    {
        final CreateSessionResponse createSessionResponse = authorizedAllocatorBlockingStub.createSession(
            CreateSessionRequest.newBuilder().setOwner(UUID.randomUUID().toString()).setCachePolicy(
                    CachePolicy.newBuilder().setIdleTimeout(Duration.newBuilder().setSeconds(0).build()).build())
                .build());

        final var future = awaitAllocationRequest();

        final Operation allocate = authorizedAllocatorBlockingStub.allocate(AllocateRequest.newBuilder()
            .setSessionId(createSessionResponse.getSessionId())
            .setPoolLabel("S")
            .build());
        final VmAllocatorApi.AllocateMetadata allocateMetadata =
            allocate.getMetadata().unpack(VmAllocatorApi.AllocateMetadata.class);

        final String podName = future.get();
        mockGetPod(podName);
        final CountDownLatch kuberRemoveRequestLatch = new CountDownLatch(1);
        mockDeletePod(podName, kuberRemoveRequestLatch::countDown, HttpURLConnection.HTTP_OK);

        //noinspection ResultOfMethodCallIgnored
        authorizedAllocatorBlockingStub.deleteSession(
            DeleteSessionRequest.newBuilder().setSessionId(createSessionResponse.getSessionId()).build());
        try {
            //noinspection ResultOfMethodCallIgnored
            privateAllocatorBlockingStub.register(
                VmAllocatorPrivateApi.RegisterRequest.newBuilder().setVmId(allocateMetadata.getVmId()).build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.FAILED_PRECONDITION.getCode(),
                e.getStatus().getCode());
        }

        Assert.assertTrue(kuberRemoveRequestLatch.await(TIMEOUT_SEC, TimeUnit.SECONDS));
    }

    @Test
    public void freeNonexistentVmTest() {
        try {
            //noinspection ResultOfMethodCallIgnored
            authorizedAllocatorBlockingStub.free(
                FreeRequest.newBuilder().setVmId(UUID.randomUUID().toString()).build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.NOT_FOUND.getCode(), e.getStatus().getCode());
        }
    }

    @Test
    public void repeatedFreeTest() throws InvalidProtocolBufferException,
            InterruptedException, ExecutionException
    {
        final CreateSessionResponse createSessionResponse = authorizedAllocatorBlockingStub.createSession(
            CreateSessionRequest.newBuilder().setOwner(UUID.randomUUID().toString()).setCachePolicy(
                    CachePolicy.newBuilder().setIdleTimeout(Duration.newBuilder().setSeconds(0).build()).build())
                .build());

        final var future = awaitAllocationRequest();

        final Operation allocationStarted = authorizedAllocatorBlockingStub.allocate(AllocateRequest.newBuilder()
            .setSessionId(createSessionResponse.getSessionId())
            .setPoolLabel("S")
            .build());
        final VmAllocatorApi.AllocateMetadata allocateMetadata =
            allocationStarted.getMetadata().unpack(VmAllocatorApi.AllocateMetadata.class);

        final String podName = future.get();
        mockGetPod(podName);
        final CountDownLatch kuberRemoveRequestLatch = new CountDownLatch(1);
        mockDeletePod(podName, kuberRemoveRequestLatch::countDown, HttpURLConnection.HTTP_OK);

        registerVm(allocateMetadata.getVmId());
        waitOp(allocationStarted);
        //noinspection ResultOfMethodCallIgnored
        authorizedAllocatorBlockingStub.free(FreeRequest.newBuilder().setVmId(allocateMetadata.getVmId()).build());

        try {
            //noinspection ResultOfMethodCallIgnored
            authorizedAllocatorBlockingStub.free(FreeRequest.newBuilder().setVmId(allocateMetadata.getVmId()).build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.FAILED_PRECONDITION.getCode(),
                e.getStatus().getCode());
        }
        Assert.assertTrue(kuberRemoveRequestLatch.await(TIMEOUT_SEC, TimeUnit.SECONDS));
    }

    @Test
    public void repeatedServantRegister() throws InvalidProtocolBufferException,
            InterruptedException, ExecutionException
    {
        final CreateSessionResponse createSessionResponse = authorizedAllocatorBlockingStub.createSession(
            CreateSessionRequest.newBuilder().setOwner(UUID.randomUUID().toString()).setCachePolicy(
                    CachePolicy.newBuilder().setIdleTimeout(Duration.newBuilder().setSeconds(0).build()).build())
                .build());
        final var future = awaitAllocationRequest();
        final Operation allocate = authorizedAllocatorBlockingStub.allocate(AllocateRequest.newBuilder()
            .setSessionId(createSessionResponse.getSessionId())
            .setPoolLabel("S")
            .build());
        final VmAllocatorApi.AllocateMetadata allocateMetadata =
            allocate.getMetadata().unpack(VmAllocatorApi.AllocateMetadata.class);

        final String podName = future.get();
        mockGetPod(podName);
        final CountDownLatch kuberRemoveRequestLatch = new CountDownLatch(1);
        mockDeletePod(podName, kuberRemoveRequestLatch::countDown, HttpURLConnection.HTTP_OK);

        registerVm(allocateMetadata.getVmId());
        try {
            //noinspection ResultOfMethodCallIgnored
            privateAllocatorBlockingStub.register(
                VmAllocatorPrivateApi.RegisterRequest.newBuilder().setVmId(allocateMetadata.getVmId()).build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.ALREADY_EXISTS.getCode(),
                e.getStatus().getCode());
        }

        //noinspection ResultOfMethodCallIgnored
        authorizedAllocatorBlockingStub.free(FreeRequest.newBuilder().setVmId(allocateMetadata.getVmId()).build());
        Assert.assertTrue(kuberRemoveRequestLatch.await(TIMEOUT_SEC, TimeUnit.SECONDS));
    }

    private void mockGetPod(String podName) {
        final Pod pod = new Pod();
        pod.setMetadata(new ObjectMetaBuilder().withName(podName).build());
        pod.setSpec(new PodSpecBuilder()
            .withNodeName("node")
            .build());
        final var mock = kubernetesServer.expect().get()
            .withPath("/api/v1/namespaces/default/pods?labelSelector=" +
                URLEncoder.encode(KuberLabels.LZY_POD_NAME_LABEL + "=" + podName, StandardCharsets.UTF_8))
            .andReturn(HttpURLConnection.HTTP_OK, new PodListBuilder().withItems(pod).build())
            .always();
    }

    private Future<String> awaitAllocationRequest() {
        final var future = new CompletableFuture<String>();
        kubernetesServer.expect().post()
            .withPath("/api/v1/namespaces/default/pods")
            .andReply(HttpURLConnection.HTTP_CREATED, (req) -> {
                final var pod = Serialization.unmarshal(
                    new ByteArrayInputStream(req.getBody().readByteArray()), Pod.class, Map.of());
                future.complete(pod.getMetadata().getName());
                return pod;
            })
            .once();
        return future;
    }

    private void mockDeletePod(String podName, Runnable onDelete, int responseCode) {
        kubernetesServer.expect().delete()
            .withPath("/api/v1/namespaces/default/pods/" + podName)
            .andReply(responseCode, (req) -> {
                onDelete.run();
                return new StatusDetails();
            }).once();
    }

    private void registerVm(String vmId) {
        TimeUtils.waitFlagUp(() -> {
            try {
                //noinspection ResultOfMethodCallIgnored
                privateAllocatorBlockingStub.register(
                    VmAllocatorPrivateApi.RegisterRequest.newBuilder().setVmId(vmId).build());
                return true;
            } catch (StatusRuntimeException e) {
                if (e.getStatus().getCode() == Status.Code.FAILED_PRECONDITION) {
                    return false;
                }
                throw new RuntimeException(e);
            }
        }, TIMEOUT_SEC, TimeUnit.SECONDS);
    }

    private Operation waitOp(Operation operation) {
        return waitOperation(operationServiceApiBlockingStub, operation, TIMEOUT_SEC);
    }
}
