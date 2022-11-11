package ai.lzy.allocator.test;

import ai.lzy.allocator.AllocatorMain;
import ai.lzy.allocator.alloc.impl.kuber.KuberClientFactory;
import ai.lzy.allocator.alloc.impl.kuber.KuberLabels;
import ai.lzy.allocator.alloc.impl.kuber.KuberVmAllocator;
import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.dao.impl.AllocatorDataSource;
import ai.lzy.allocator.dao.impl.SessionDaoImpl;
import ai.lzy.allocator.vmpool.ClusterRegistry;
import ai.lzy.iam.resources.subjects.AuthProvider;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.iam.test.BaseTestWithIam;
import ai.lzy.model.db.test.DatabaseTestUtils;
import ai.lzy.test.TimeUtils;
import ai.lzy.util.auth.credentials.JwtUtils;
import ai.lzy.util.grpc.ClientHeaderInterceptor;
import ai.lzy.util.grpc.GrpcHeaders;
import ai.lzy.v1.*;
import ai.lzy.v1.VmAllocatorApi.*;
import ai.lzy.v1.longrunning.LongRunning.Operation;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import com.google.protobuf.Duration;
import com.google.protobuf.util.Durations;
import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.client.server.mock.KubernetesServer;
import io.fabric8.kubernetes.client.utils.Serialization;
import io.grpc.ManagedChannel;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;

import static ai.lzy.allocator.alloc.impl.kuber.KuberVmAllocator.*;
import static ai.lzy.allocator.test.Utils.waitOperation;
import static ai.lzy.allocator.volume.KuberVolumeManager.KUBER_GB_NAME;
import static ai.lzy.allocator.volume.KuberVolumeManager.VOLUME_CAPACITY_STORAGE_KEY;
import static ai.lzy.allocator.volume.KuberVolumeManager.YCLOUD_DISK_DRIVER;
import static ai.lzy.allocator.volume.Volume.AccessMode.READ_WRITE_ONCE;
import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;
import static ai.lzy.util.grpc.GrpcUtils.withIdempotencyKey;
import static java.util.Objects.requireNonNull;

public class AllocatorApiTest extends BaseTestWithIam {

    private static final int TIMEOUT_SEC = 300;

    private static final String POD_PATH = "/api/v1/namespaces/%s/pods".formatted(NAMESPACE);
    private static final String PERSISTENT_VOLUME_PATH = "/api/v1/persistentvolumes";
    private static final String PERSISTENT_VOLUME_CLAIM_PATH = "/api/v1/namespaces/%s/persistentvolumeclaims"
        .formatted(NAMESPACE);
    private static final String ZONE = "test-zone";
    private static final ClusterRegistry.ClusterType CLUSTER_TYPE = ClusterRegistry.ClusterType.User;

    @Rule
    public PreparedDbRule iamDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});
    @Rule
    public PreparedDbRule db = EmbeddedPostgresRules.preparedDatabase(ds -> {});

    private ApplicationContext allocatorCtx;
    private AllocatorGrpc.AllocatorBlockingStub unauthorizedAllocatorBlockingStub;
    private AllocatorGrpc.AllocatorBlockingStub authorizedAllocatorBlockingStub;
    private AllocatorPrivateGrpc.AllocatorPrivateBlockingStub privateAllocatorBlockingStub;
    private LongRunningServiceGrpc.LongRunningServiceBlockingStub operationServiceApiBlockingStub;
    private DiskServiceGrpc.DiskServiceBlockingStub diskService;
    private AllocatorMain allocatorApp;
    private KubernetesServer kubernetesServer;
    private ManagedChannel channel;
    private ClusterRegistry clusterRegistry;

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

        channel = newGrpcChannel(config.getAddress(), AllocatorGrpc.SERVICE_NAME, AllocatorPrivateGrpc.SERVICE_NAME,
            LongRunningServiceGrpc.SERVICE_NAME, DiskServiceGrpc.SERVICE_NAME);

        var credentials = config.getIam().createRenewableToken();
        unauthorizedAllocatorBlockingStub = AllocatorGrpc.newBlockingStub(channel);
        privateAllocatorBlockingStub = newBlockingClient(AllocatorPrivateGrpc.newBlockingStub(channel), "Test",
            () -> credentials.get().token());
        operationServiceApiBlockingStub = newBlockingClient(LongRunningServiceGrpc.newBlockingStub(channel), "Test",
            () -> credentials.get().token());
        authorizedAllocatorBlockingStub = newBlockingClient(unauthorizedAllocatorBlockingStub, "Test",
            () -> credentials.get().token());
        diskService = newBlockingClient(DiskServiceGrpc.newBlockingStub(channel), "Test",
            () -> credentials.get().token());

        clusterRegistry = allocatorCtx.getBean(ClusterRegistry.class);
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
                    JwtUtils.invalidCredentials("user", "GITHUB")::token));
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
        var sessionId = createSession(Durations.fromSeconds(100));

        var deleteSessionResponse = authorizedAllocatorBlockingStub.deleteSession(
            DeleteSessionRequest.newBuilder()
                .setSessionId(sessionId)
                .build());

        Assert.assertNotNull(deleteSessionResponse);
    }

    @Test
    public void createSessionNoOwner() {
        try {
            //noinspection ResultOfMethodCallIgnored
            authorizedAllocatorBlockingStub.createSession(
                CreateSessionRequest.newBuilder()
                    .setCachePolicy(
                        CachePolicy.newBuilder()
                            .setIdleTimeout(Durations.fromSeconds(100))
                            .build())
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
                CreateSessionRequest.newBuilder()
                    .setOwner(UUID.randomUUID().toString())
                    .build());
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
                CreateSessionRequest.newBuilder()
                    .setOwner(UUID.randomUUID().toString())
                    .setCachePolicy(
                        CachePolicy.newBuilder()
                            .build())
                    .build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.INVALID_ARGUMENT.getCode(), e.getStatus().getCode());
        }
    }

    @Test
    public void errorWhileCreatingSession() {
        allocatorCtx.getBean(SessionDaoImpl.class).injectError(new SQLException("non retryable", "xxx"));

        try {
            var sessionId = createSession(Durations.fromSeconds(100));
            Assert.fail(sessionId);
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(Status.Code.INTERNAL, e.getStatus().getCode());
            Assert.assertEquals("non retryable", e.getStatus().getDescription());
        }
    }

    @Test
    public void retryableSqlErrorWhileCreatingSession() {
        allocatorCtx.getBean(SessionDaoImpl.class).injectError(
            new PSQLException("retry me, plz", PSQLState.CONNECTION_FAILURE));

        createSession(Durations.fromSeconds(100));
    }

    @Test
    public void idempotentCreateSessionRepeatableCalls() {
        var op1 = createSessionOp("test", Durations.ZERO, "key-1");
        var op2 = createSessionOp("test", Durations.ZERO, "key-1");

        Assert.assertEquals(op1.getId(), op2.getId());
        Assert.assertEquals(Utils.extractSessionId(op1), Utils.extractSessionId(op2));
    }

    @Test
    public void idempotentCreateSessionRepeatableCallsWithModification() {
        createSessionOp("test", Durations.ZERO, "key-1");

        try {
            createSessionOp("test1", Durations.ZERO, "key-1");
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(Status.INVALID_ARGUMENT.getCode(), e.getStatus().getCode());
            Assert.assertEquals("IdempotencyKey conflict", e.getStatus().getDescription());
        }

        try {
            createSessionOp("test", Durations.fromSeconds(1), "key-1");
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(Status.INVALID_ARGUMENT.getCode(), e.getStatus().getCode());
            Assert.assertEquals("IdempotencyKey conflict", e.getStatus().getDescription());
        }
    }

    @Test
    public void idempotentCreateSessionConcurrentCalls() throws Exception {
        final int N = 10;

        var readyLatch = new CountDownLatch(N);
        var startLatch = new CountDownLatch(1);
        var completedLatch = new CountDownLatch(N);
        var sids = new String[N];

        var oks = new AtomicInteger(0);
        var errors = new AtomicInteger(0);

        var executor = Executors.newFixedThreadPool(N);
        for (int i = 0; i < N; ++i) {
            final int index = i;
            executor.submit(() -> {
                readyLatch.countDown();
                try {
                    startLatch.await();
                } catch (InterruptedException e) {
                    completedLatch.countDown();
                    throw new RuntimeException(e);
                }
                try {
                    var op = createSessionOp("test", Durations.ZERO, "key-1");
                    var sid = Utils.extractSessionId(op);
                    sids[index] = sid;

                    oks.getAndIncrement();
                } catch (Exception e) {
                    errors.getAndIncrement();
                    System.err.println("Exception in thread " + Thread.currentThread().getName());
                    e.printStackTrace(System.err);
                }
                completedLatch.countDown();
            });
        }

        readyLatch.await();
        startLatch.countDown();

        completedLatch.await();
        executor.shutdown();

        Assert.assertEquals(N, oks.get());
        Assert.assertEquals(0, errors.get());

        Assert.assertNotNull(sids[0]);
        var sid = sids[0];
        for (int i = 1; i < N; ++i) {
            Assert.assertEquals(sid, sids[i]);
        }
    }

    @Test
    public void allocateKuberErrorWhileCreateTest() throws Exception {
        //simulate kuber api error on pod creation
        kubernetesServer.getKubernetesMockServer().clearExpectations();
        kubernetesServer.expect().post().withPath(POD_PATH)
            .andReturn(HttpURLConnection.HTTP_INTERNAL_ERROR, new PodListBuilder().build())
            .once();

        var sessionId = createSession(Durations.fromSeconds(100));

        final Operation operation = authorizedAllocatorBlockingStub.allocate(
            AllocateRequest.newBuilder()
                .setSessionId(sessionId)
                .setPoolLabel("S")
                .build());
        final VmAllocatorApi.AllocateMetadata allocateMetadata =
            operation.getMetadata().unpack(VmAllocatorApi.AllocateMetadata.class);

        final String podName = KuberVmAllocator.VM_POD_NAME_PREFIX + allocateMetadata.getVmId();
        mockGetPod(podName);
        final CountDownLatch kuberRemoveRequestLatch = new CountDownLatch(1);
        mockDeletePod(podName, kuberRemoveRequestLatch::countDown, HttpURLConnection.HTTP_OK);

        waitOpError(operation, Status.INTERNAL);
        Assert.assertTrue(kuberRemoveRequestLatch.await(TIMEOUT_SEC, TimeUnit.SECONDS));
    }

    @Test
    public void allocateServantTimeoutTest() throws Exception {
        var sessionId = createSession(Durations.fromSeconds(100));

        final var future = awaitAllocationRequest();

        final Operation operation = authorizedAllocatorBlockingStub.allocate(
            AllocateRequest.newBuilder()
                .setSessionId(sessionId)
                .setPoolLabel("S")
                .build());

        final String podName = future.get();
        mockGetPod(podName);
        
        final CountDownLatch kuberRemoveRequestLatch = new CountDownLatch(1);
        mockDeletePod(podName, kuberRemoveRequestLatch::countDown, HttpURLConnection.HTTP_OK);

        waitOpError(operation, Status.DEADLINE_EXCEEDED);
        Assert.assertTrue(kuberRemoveRequestLatch.await(TIMEOUT_SEC, TimeUnit.SECONDS));
    }

    @Test
    public void allocateInvalidPoolTest() {
        var sessionId = createSession(Durations.fromSeconds(100));

        waitOpError(
            authorizedAllocatorBlockingStub.allocate(
                AllocateRequest.newBuilder()
                    .setSessionId(sessionId)
                    .setPoolLabel("LABEL")
                    .build()),
            Status.INVALID_ARGUMENT);
    }

    @Test
    public void allocateInvalidSessionTest() {
        try {
            waitOperation(
                operationServiceApiBlockingStub,
                authorizedAllocatorBlockingStub.allocate(
                    AllocateRequest.newBuilder()
                        .setSessionId(UUID.randomUUID().toString())
                        .setPoolLabel("S")
                        .build()),
                TIMEOUT_SEC
            );
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.INVALID_ARGUMENT.getCode(), e.getStatus().getCode());
        }
    }

    @Test
    public void allocateFreeSuccessTest() throws Exception {
        var sessionId = createSession(Durations.ZERO);

        var future = awaitAllocationRequest();

        var allocationStarted = authorizedAllocatorBlockingStub.allocate(
            AllocateRequest.newBuilder()
                .setSessionId(sessionId)
                .setPoolLabel("S")
                .build());
        var allocateMetadata = allocationStarted.getMetadata().unpack(VmAllocatorApi.AllocateMetadata.class);

        final String podName = future.get();
        mockGetPod(podName);
        final CountDownLatch kuberRemoveRequestLatch = new CountDownLatch(1);
        mockDeletePod(podName, kuberRemoveRequestLatch::countDown, HttpURLConnection.HTTP_OK);

        String clusterId = requireNonNull(clusterRegistry.findCluster("S", ZONE, CLUSTER_TYPE)).clusterId();
        registerVm(allocateMetadata.getVmId(), clusterId);

        final Operation allocate = waitOpSuccess(allocationStarted);
        final VmAllocatorApi.AllocateResponse allocateResponse =
            allocate.getResponse().unpack(VmAllocatorApi.AllocateResponse.class);

        Assert.assertEquals(sessionId, allocateResponse.getSessionId());
        Assert.assertEquals("S", allocateResponse.getPoolId());

        var vmSubj = super.getSubject(AuthProvider.INTERNAL, allocateMetadata.getVmId(), SubjectType.VM);
        Assert.assertNotNull(vmSubj);

        //noinspection ResultOfMethodCallIgnored
        authorizedAllocatorBlockingStub.free(FreeRequest.newBuilder().setVmId(allocateMetadata.getVmId()).build());
        Assert.assertTrue(kuberRemoveRequestLatch.await(TIMEOUT_SEC, TimeUnit.SECONDS));

        vmSubj = super.getSubject(AuthProvider.INTERNAL, allocateMetadata.getVmId(), SubjectType.VM);
        Assert.assertNull(vmSubj);
    }

    @Test
    public void eventualFreeAfterFailTest() throws Exception {
        var sessionId = createSession(Durations.ZERO);

        var future = awaitAllocationRequest();

        var allocationStarted = authorizedAllocatorBlockingStub.allocate(
            AllocateRequest.newBuilder()
                .setSessionId(sessionId)
                .setPoolLabel("S")
                .build());
        var allocateMetadata = allocationStarted.getMetadata().unpack(VmAllocatorApi.AllocateMetadata.class);

        final String podName = future.get();
        mockGetPod(podName);
        final CountDownLatch kuberRemoveRequestLatch = new CountDownLatch(2);
        mockDeletePod(podName, () -> {
            kuberRemoveRequestLatch.countDown();
            mockDeletePod(podName, kuberRemoveRequestLatch::countDown, HttpURLConnection.HTTP_OK);
        }, HttpURLConnection.HTTP_INTERNAL_ERROR);

        String clusterId = requireNonNull(clusterRegistry.findCluster("S", ZONE, CLUSTER_TYPE)).clusterId();
        registerVm(allocateMetadata.getVmId(), clusterId);

        final Operation allocate = waitOpSuccess(allocationStarted);
        final VmAllocatorApi.AllocateResponse allocateResponse =
            allocate.getResponse().unpack(VmAllocatorApi.AllocateResponse.class);

        Assert.assertEquals(sessionId, allocateResponse.getSessionId());
        Assert.assertEquals("S", allocateResponse.getPoolId());

        var vmSubj = super.getSubject(AuthProvider.INTERNAL, allocateMetadata.getVmId(), SubjectType.VM);
        Assert.assertNotNull(vmSubj);

        //noinspection ResultOfMethodCallIgnored
        authorizedAllocatorBlockingStub.free(FreeRequest.newBuilder().setVmId(allocateMetadata.getVmId()).build());
        Assert.assertTrue(kuberRemoveRequestLatch.await(TIMEOUT_SEC, TimeUnit.SECONDS));

        vmSubj = super.getSubject(AuthProvider.INTERNAL, allocateMetadata.getVmId(), SubjectType.VM);
        Assert.assertNull(vmSubj);
    }

    @Test
    public void allocateFromCacheTest() throws Exception {
        var sessionId = createSession(Durations.fromSeconds(5));

        final var future = awaitAllocationRequest();

        var operationFirst = authorizedAllocatorBlockingStub.allocate(
            AllocateRequest.newBuilder()
                .setSessionId(sessionId)
                .setPoolLabel("S")
                .build());
        var allocateMetadataFirst = operationFirst.getMetadata().unpack(VmAllocatorApi.AllocateMetadata.class);

        final String firstPodName = future.get();
        mockGetPod(firstPodName);

        String clusterId = requireNonNull(clusterRegistry.findCluster("S", ZONE, CLUSTER_TYPE)).clusterId();
        registerVm(allocateMetadataFirst.getVmId(), clusterId);

        operationFirst = waitOpSuccess(operationFirst);

        var vmSubj1 = super.getSubject(AuthProvider.INTERNAL, allocateMetadataFirst.getVmId(), SubjectType.VM);
        Assert.assertNotNull(vmSubj1);

        //noinspection ResultOfMethodCallIgnored
        authorizedAllocatorBlockingStub.free(FreeRequest.newBuilder().setVmId(allocateMetadataFirst.getVmId()).build());

        Operation operationSecond = authorizedAllocatorBlockingStub.allocate(
            AllocateRequest.newBuilder()
                .setSessionId(sessionId)
                .setPoolLabel("S")
                .build());
        final VmAllocatorApi.AllocateMetadata allocateMetadataSecond =
            operationSecond.getMetadata().unpack(VmAllocatorApi.AllocateMetadata.class);

        final CountDownLatch kuberRemoveRequestLatch = new CountDownLatch(1);
        mockDeletePod(firstPodName, kuberRemoveRequestLatch::countDown, HttpURLConnection.HTTP_OK);
        operationSecond = waitOpSuccess(operationSecond);

        final var allocateResponseFirst = operationFirst.getResponse().unpack(AllocateResponse.class);
        final var allocateResponseSecond = operationSecond.getResponse().unpack(AllocateResponse.class);
        Assert.assertEquals(allocateResponseFirst.getVmId(), allocateResponseSecond.getVmId());

        var vmSubj2 = super.getSubject(AuthProvider.INTERNAL, allocateMetadataSecond.getVmId(), SubjectType.VM);
        Assert.assertNotNull(vmSubj2);

        Assert.assertEquals(allocateMetadataFirst.getVmId(), allocateMetadataSecond.getVmId());
        Assert.assertEquals(vmSubj1, vmSubj2);

        //noinspection ResultOfMethodCallIgnored
        authorizedAllocatorBlockingStub.free(
            FreeRequest.newBuilder().setVmId(allocateMetadataSecond.getVmId()).build());

        Assert.assertEquals(allocateMetadataFirst.getVmId(), allocateMetadataSecond.getVmId());
        Assert.assertTrue(kuberRemoveRequestLatch.await(TIMEOUT_SEC, TimeUnit.SECONDS));

        var vmSubj = super.getSubject(AuthProvider.INTERNAL, allocateMetadataFirst.getVmId(), SubjectType.VM);
        Assert.assertNull(vmSubj);
    }

    @Test
    public void deleteSessionWithActiveVmsAfterRegister() throws Exception {
        var sessionId = createSession(Durations.ZERO);

        var future = awaitAllocationRequest();

        var allocate = authorizedAllocatorBlockingStub.allocate(
            AllocateRequest.newBuilder()
                .setSessionId(sessionId)
                .setPoolLabel("S")
                .build());
        var allocateMetadata = allocate.getMetadata().unpack(VmAllocatorApi.AllocateMetadata.class);

        final String podName = future.get();
        mockGetPod(podName);
        final CountDownLatch kuberRemoveRequestLatch = new CountDownLatch(1);
        mockDeletePod(podName, kuberRemoveRequestLatch::countDown, HttpURLConnection.HTTP_OK);

        String clusterId = requireNonNull(clusterRegistry.findCluster("S", ZONE, CLUSTER_TYPE)).clusterId();
        registerVm(allocateMetadata.getVmId(), clusterId);

        //noinspection ResultOfMethodCallIgnored
        authorizedAllocatorBlockingStub.deleteSession(
            DeleteSessionRequest.newBuilder()
                .setSessionId(sessionId)
                .build());

        Assert.assertTrue(kuberRemoveRequestLatch.await(TIMEOUT_SEC, TimeUnit.SECONDS));

        var vmSubj = super.getSubject(AuthProvider.INTERNAL, allocateMetadata.getVmId(), SubjectType.VM);
        Assert.assertNull(vmSubj);
    }

    @Test
    public void deleteSessionWithActiveVmsBeforeRegister() throws Exception {
        var sessionId = createSession(Durations.ZERO);

        var future = awaitAllocationRequest();

        var allocate = authorizedAllocatorBlockingStub.allocate(
            AllocateRequest.newBuilder()
                .setSessionId(sessionId)
                .setPoolLabel("S")
                .build());
        var allocateMetadata = allocate.getMetadata().unpack(VmAllocatorApi.AllocateMetadata.class);

        final String podName = future.get();
        mockGetPod(podName);
        final CountDownLatch kuberRemoveRequestLatch = new CountDownLatch(1);
        mockDeletePod(podName, kuberRemoveRequestLatch::countDown, HttpURLConnection.HTTP_OK);

        //noinspection ResultOfMethodCallIgnored
        authorizedAllocatorBlockingStub.deleteSession(
            DeleteSessionRequest.newBuilder()
                .setSessionId(sessionId)
                .build());

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
    public void repeatedFreeTest() throws Exception {
        var sessionId = createSession(Durations.ZERO);

        var future = awaitAllocationRequest();

        var allocationStarted = authorizedAllocatorBlockingStub.allocate(
            AllocateRequest.newBuilder()
                .setSessionId(sessionId)
                .setPoolLabel("S")
                .build());
        var allocateMetadata = allocationStarted.getMetadata().unpack(VmAllocatorApi.AllocateMetadata.class);

        final String podName = future.get();
        mockGetPod(podName);
        final CountDownLatch kuberRemoveRequestLatch = new CountDownLatch(1);
        mockDeletePod(podName, kuberRemoveRequestLatch::countDown, HttpURLConnection.HTTP_OK);

        String clusterId = requireNonNull(clusterRegistry.findCluster("S", ZONE, CLUSTER_TYPE)).clusterId();
        registerVm(allocateMetadata.getVmId(), clusterId);

        waitOpSuccess(allocationStarted);
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
    public void repeatedServantRegister() throws Exception {
        var sessionId = createSession(Durations.ZERO);

        var future = awaitAllocationRequest();

        var allocate = authorizedAllocatorBlockingStub.allocate(
            AllocateRequest.newBuilder()
                .setSessionId(sessionId)
                .setPoolLabel("S")
                .build());
        var allocateMetadata = allocate.getMetadata().unpack(VmAllocatorApi.AllocateMetadata.class);

        final String podName = future.get();
        mockGetPod(podName);
        final CountDownLatch kuberRemoveRequestLatch = new CountDownLatch(1);
        mockDeletePod(podName, kuberRemoveRequestLatch::countDown, HttpURLConnection.HTTP_OK);

        String clusterId = requireNonNull(clusterRegistry.findCluster("S", ZONE, CLUSTER_TYPE)).clusterId();
        registerVm(allocateMetadata.getVmId(), clusterId);

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

    @Test
    public void runWithVolumes() throws Exception {
        var sessionId = createSession(Durations.ZERO);

        var future = awaitAllocationRequest();

        final String volumeName = "volumeName";
        final String mountPath = "/mnt/volume";
        final VolumeApi.Mount volumeMount = VolumeApi.Mount.newBuilder()
            .setVolumeName(volumeName)
            .setMountPath(mountPath)
            .setReadOnly(false)
            .setMountPropagation(VolumeApi.Mount.MountPropagation.NONE)
            .build();

        final DiskApi.DiskSpec diskSpec = DiskApi.DiskSpec.newBuilder()
            .setName("disk-name")
            .setSizeGb(4)
            .setZoneId("ru-central1-a")
            .setType(DiskApi.DiskType.HDD)
            .build();
        Operation createDiskOperation = diskService.createDisk(DiskServiceApi.CreateDiskRequest.newBuilder()
            .setUserId("user-id")
            .setDiskSpec(diskSpec)
            .build());
        createDiskOperation = waitOpSuccess(createDiskOperation);
        Assert.assertTrue(createDiskOperation.hasResponse());
        final DiskServiceApi.CreateDiskResponse createDiskResponse = createDiskOperation.getResponse().unpack(
            DiskServiceApi.CreateDiskResponse.class);
        final DiskApi.Disk disk = createDiskResponse.getDisk();

        final var persistentVolumeFuture = awaitResourceCreate(PersistentVolume.class, PERSISTENT_VOLUME_PATH);
        final var persistentVolumeClaimFuture =
            awaitResourceCreate(PersistentVolumeClaim.class, PERSISTENT_VOLUME_CLAIM_PATH);

        final Operation allocationStarted = authorizedAllocatorBlockingStub.allocate(
            AllocateRequest.newBuilder()
                .setSessionId(sessionId)
                .setPoolLabel("S")
                .addWorkload(AllocateRequest.Workload.newBuilder()
                    .setName("workload")
                    .addVolumeMounts(volumeMount)
                    .build())
                .addVolumes(VolumeApi.Volume.newBuilder()
                    .setName(volumeName)
                    .setDiskVolume(VolumeApi.DiskVolumeType.newBuilder()
                        .setDiskId(disk.getDiskId()).build())
                    .build())
                .build());
        var allocateMetadata = allocationStarted.getMetadata().unpack(VmAllocatorApi.AllocateMetadata.class);

        final String podName = future.get();
        mockGetPod(podName);

        final PersistentVolume persistentVolume = persistentVolumeFuture.get();
        final PersistentVolumeClaim persistentVolumeClaim = persistentVolumeClaimFuture.get();
        final PersistentVolumeSpec volumeSpec = persistentVolume.getSpec();
        Assert.assertEquals(disk.getDiskId(), volumeSpec.getCsi().getVolumeHandle());
        Assert.assertEquals(YCLOUD_DISK_DRIVER, volumeSpec.getCsi().getDriver());
        final Quantity expectedDiskSize = new Quantity(disk.getSpec().getSizeGb() + KUBER_GB_NAME);
        Assert.assertEquals(
            expectedDiskSize,
            volumeSpec.getCapacity().get("storage")
        );

        Assert.assertEquals(persistentVolume.getMetadata().getName(), persistentVolumeClaim.getSpec().getVolumeName());
        Assert.assertEquals("", persistentVolumeClaim.getSpec().getStorageClassName());
        Assert.assertEquals(READ_WRITE_ONCE.asString(), persistentVolumeClaim.getSpec().getAccessModes().get(0));
        Assert.assertEquals(expectedDiskSize,
            persistentVolumeClaim.getSpec().getResources().getRequests().get(VOLUME_CAPACITY_STORAGE_KEY));

        final CountDownLatch kuberRemoveResourceLatch = new CountDownLatch(3);
        mockDeletePod(podName, kuberRemoveResourceLatch::countDown, HttpURLConnection.HTTP_OK);
        mockDeleteResource(
            PERSISTENT_VOLUME_PATH,
            persistentVolume.getMetadata().getName(),
            kuberRemoveResourceLatch::countDown,
            HttpURLConnection.HTTP_OK
        );
        mockDeleteResource(
            PERSISTENT_VOLUME_CLAIM_PATH,
            persistentVolumeClaim.getMetadata().getName(),
            kuberRemoveResourceLatch::countDown,
            HttpURLConnection.HTTP_OK
        );

        String clusterId = requireNonNull(clusterRegistry.findCluster("S", ZONE, CLUSTER_TYPE)).clusterId();
        registerVm(allocateMetadata.getVmId(), clusterId);

        waitOpSuccess(allocationStarted);
        //noinspection ResultOfMethodCallIgnored
        authorizedAllocatorBlockingStub.free(FreeRequest.newBuilder().setVmId(allocateMetadata.getVmId()).build());
        Assert.assertTrue(kuberRemoveResourceLatch.await(TIMEOUT_SEC, TimeUnit.SECONDS));

        //noinspection ResultOfMethodCallIgnored
        diskService.deleteDisk(DiskServiceApi.DeleteDiskRequest.newBuilder().setDiskId(disk.getDiskId()).build());
    }

    @Test
    public void runWithVolumeButNonExistingDisk() {
        var sessionId = createSession(Durations.ZERO);

        final String volumeName = "volumeName";
        final String mountPath = "/mnt/volume";
        final VolumeApi.Mount volumeMount = VolumeApi.Mount.newBuilder()
            .setVolumeName(volumeName)
            .setMountPath(mountPath)
            .setReadOnly(false)
            .setMountPropagation(VolumeApi.Mount.MountPropagation.NONE)
            .build();

        try {
            Operation allocationStarted = authorizedAllocatorBlockingStub.allocate(
                AllocateRequest.newBuilder()
                    .setSessionId(sessionId)
                    .setPoolLabel("S")
                    .addWorkload(AllocateRequest.Workload.newBuilder()
                        .setName("workload")
                        .addVolumeMounts(volumeMount)
                        .build())
                    .addVolumes(VolumeApi.Volume.newBuilder()
                        .setName(volumeName)
                        .setDiskVolume(VolumeApi.DiskVolumeType.newBuilder()
                            .setDiskId("unknown-disk-id").build())
                        .build())
                    .build());
            waitOpError(allocationStarted, Status.NOT_FOUND);
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(Status.NOT_FOUND.getCode(), e.getStatus().getCode());
        }
    }

    private void mockGetPod(String podName) {
        final Pod pod = new Pod();
        pod.setMetadata(
            new ObjectMetaBuilder()
                .withName(podName)
                .withLabels(
                    Map.of(
                        KuberLabels.LZY_VM_ID_LABEL, podName.substring(VM_POD_NAME_PREFIX.length())))
                .build()
        );
        pod.setSpec(new PodSpecBuilder()
            .withNodeName("node")
            .build());
        kubernetesServer.expect().get()
            .withPath(POD_PATH + "?labelSelector=" +
                URLEncoder.encode(KuberLabels.LZY_POD_NAME_LABEL + "=" + podName, StandardCharsets.UTF_8))
            .andReturn(HttpURLConnection.HTTP_OK, new PodListBuilder().withItems(pod).build())
            .always();
    }

    private <T> Future<T> awaitResourceCreate(Class<T> resourceType, String resourcePath) {
        final var future = new CompletableFuture<T>();
        kubernetesServer.expect().post()
            .withPath(resourcePath)
            .andReply(HttpURLConnection.HTTP_CREATED, (req) -> {
                final var resource = Serialization.unmarshal(
                    new ByteArrayInputStream(req.getBody().readByteArray()), resourceType, Map.of());
                future.complete(resource);
                return resource;
            })
            .once();
        return future;
    }

    private Future<String> awaitAllocationRequest() {
        final var future = new CompletableFuture<String>();
        kubernetesServer.expect().post()
            .withPath(POD_PATH)
            .andReply(HttpURLConnection.HTTP_CREATED, (req) -> {
                final var pod = Serialization.unmarshal(
                    new ByteArrayInputStream(req.getBody().readByteArray()), Pod.class, Map.of());
                future.complete(pod.getMetadata().getName());
                return pod;
            })
            .once();
        return future;
    }

    private void mockDeleteResource(String resourcePath, String resourceName, Runnable onDelete, int responseCode) {
        kubernetesServer.expect().delete()
            .withPath(resourcePath + "/" + resourceName)
            .andReply(responseCode, (req) -> {
                onDelete.run();
                return new StatusDetails();
            }).once();
    }

    private void mockDeletePod(String podName, Runnable onDelete, int responseCode) {
        mockDeleteResource(POD_PATH, podName, onDelete, responseCode);
        kubernetesServer.expect().delete()
            // "lzy.ai/vm-id"=<VM id>
            .withPath(POD_PATH + "?labelSelector=lzy.ai%2Fvm-id%3D" + podName.substring(VM_POD_NAME_PREFIX.length()))
            .andReply(responseCode, (req) -> {
                onDelete.run();
                return new StatusDetails();
            }).once();
    }

    private void registerVm(String vmId, String clusterId) {
        TimeUtils.waitFlagUp(() -> {
            try {
                //noinspection ResultOfMethodCallIgnored
                privateAllocatorBlockingStub.register(
                    VmAllocatorPrivateApi.RegisterRequest.newBuilder()
                        .setVmId(vmId)
                        .putMetadata(NAMESPACE_KEY, NAMESPACE)
                        .putMetadata(CLUSTER_ID_KEY, clusterId)
                        .build()
                );
                return true;
            } catch (StatusRuntimeException e) {
                if (e.getStatus().getCode() == Status.Code.FAILED_PRECONDITION) {
                    return false;
                }
                throw new RuntimeException(e);
            }
        }, TIMEOUT_SEC, TimeUnit.SECONDS);
    }

    private Operation waitOpSuccess(Operation operation) {
        var updatedOperation = waitOperation(operationServiceApiBlockingStub, operation, TIMEOUT_SEC);
        Assert.assertTrue(updatedOperation.hasResponse());
        Assert.assertFalse(updatedOperation.hasError());
        Assert.assertTrue(updatedOperation.getDone());
        return updatedOperation;
    }

    private void waitOpError(Operation operation, Status expectedErrorStatus) {
        var updatedOperation = waitOperation(operationServiceApiBlockingStub, operation, TIMEOUT_SEC);
        Assert.assertFalse(updatedOperation.hasResponse());
        Assert.assertTrue(updatedOperation.hasError());
        Assert.assertEquals(expectedErrorStatus.getCode().value(), updatedOperation.getError().getCode());
    }

    private String createSession(Duration idleTimeout) {
        var op = createSessionOp(UUID.randomUUID().toString(), idleTimeout, null);
        return Utils.extractSessionId(op);
    }

    private Operation createSessionOp(String owner, Duration idleTimeout, @Nullable String idempotencyKey) {
        var stub = authorizedAllocatorBlockingStub;

        if (idempotencyKey != null) {
            stub = withIdempotencyKey(stub, idempotencyKey);
        }

        var op = stub.createSession(
            CreateSessionRequest.newBuilder()
                .setOwner(owner)
                .setCachePolicy(
                    CachePolicy.newBuilder()
                        .setIdleTimeout(idleTimeout)
                        .build())
                .build());
        Assert.assertTrue(op.getDone());
        return op;
    }
}
