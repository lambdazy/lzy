package ai.lzy.allocator.test;

import ai.lzy.allocator.AllocatorMain;
import ai.lzy.allocator.alloc.AllocatorMetrics;
import ai.lzy.allocator.alloc.dao.VmDao;
import ai.lzy.allocator.alloc.impl.kuber.KuberClientFactory;
import ai.lzy.allocator.alloc.impl.kuber.KuberLabels;
import ai.lzy.allocator.alloc.impl.kuber.KuberTunnelAllocator;
import ai.lzy.allocator.alloc.impl.kuber.KuberVmAllocator;
import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.gc.GarbageCollector;
import ai.lzy.allocator.model.Vm;
import ai.lzy.allocator.storage.AllocatorDataSource;
import ai.lzy.allocator.vmpool.ClusterRegistry;
import ai.lzy.common.IdGenerator;
import ai.lzy.iam.test.BaseTestWithIam;
import ai.lzy.longrunning.OperationsExecutor;
import ai.lzy.model.db.test.DatabaseTestUtils;
import ai.lzy.test.TimeUtils;
import ai.lzy.util.auth.credentials.OttHelper;
import ai.lzy.v1.AllocatorGrpc;
import ai.lzy.v1.AllocatorPrivateGrpc;
import ai.lzy.v1.DiskServiceGrpc;
import ai.lzy.v1.VmAllocatorApi;
import ai.lzy.v1.VmAllocatorPrivateApi;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import com.google.protobuf.Duration;
import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import io.fabric8.kubernetes.client.utils.Serialization;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.micronaut.context.ApplicationContext;
import io.micronaut.inject.qualifiers.Qualifiers;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import jakarta.annotation.Nullable;
import okhttp3.mockwebserver.MockWebServer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Rule;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static ai.lzy.allocator.alloc.impl.kuber.KuberVmAllocator.*;
import static ai.lzy.allocator.test.Utils.waitOperation;
import static ai.lzy.test.GrpcUtils.withGrpcContext;
import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;
import static ai.lzy.util.grpc.GrpcUtils.withIdempotencyKey;
import static java.util.Objects.requireNonNull;

public class AllocatorApiTestBase extends BaseTestWithIam {

    private static final Logger LOG = LogManager.getLogger(AllocatorApiTestBase.class);

    protected static final long TIMEOUT_SEC = 10;
    protected static final String ZONE = "test-zone";

    protected static final String POD_PATH = "/api/v1/namespaces/%s/pods".formatted(NAMESPACE_VALUE);
    protected static final String PERSISTENT_VOLUME_PATH = "/api/v1/persistentvolumes";
    protected static final String PERSISTENT_VOLUME_CLAIM_PATH = "/api/v1/namespaces/%s/persistentvolumeclaims"
        .formatted(NAMESPACE_VALUE);
    protected static final ClusterRegistry.ClusterType CLUSTER_TYPE = ClusterRegistry.ClusterType.User;

    @Rule
    public PreparedDbRule iamDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});
    @Rule
    public PreparedDbRule db = EmbeddedPostgresRules.preparedDatabase(ds -> {});

    protected ApplicationContext allocatorCtx;
    protected AllocatorGrpc.AllocatorBlockingStub unauthorizedAllocatorBlockingStub;
    protected AllocatorGrpc.AllocatorBlockingStub authorizedAllocatorBlockingStub;
    protected AllocatorPrivateGrpc.AllocatorPrivateBlockingStub privateAllocatorBlockingStub;
    protected LongRunningServiceGrpc.LongRunningServiceBlockingStub operationServiceApiBlockingStub;
    protected DiskServiceGrpc.DiskServiceBlockingStub diskService;
    protected AllocatorMain allocatorApp;
    protected KubernetesMockServer kubernetesServer;
    protected ManagedChannel channel;
    protected ClusterRegistry clusterRegistry;
    protected OperationsExecutor operationsExecutor;
    protected AllocatorMetrics metrics;
    protected GarbageCollector gc;
    protected VmDao vmDao;
    protected IdGenerator idGenerator;

    protected void updateStartupProperties(Map<String, Object> props) {}

    protected void setUp() throws IOException {
        super.setUp(DatabaseTestUtils.preparePostgresConfig("iam", iamDb.getConnectionInfo()));

        kubernetesServer = new KubernetesMockServer(new MockWebServer(), new ConcurrentHashMap<>(), false);
        kubernetesServer.init(InetAddress.getLoopbackAddress(), 0);
//        kubernetesServer.expect().post().withPath("/api/v1/pods")
//            .andReturn(HttpURLConnection.HTTP_OK, new PodListBuilder().build()).always();

        final Node node = new NodeBuilder()
            .withSpec(new NodeSpecBuilder()
                .withProviderID("yandex://node")
                .build())
            .withStatus(new NodeStatusBuilder()
                .withAddresses(new NodeAddressBuilder()
                    .withAddress("localhost")
                    .withType("HostName")
                    .build())
                .build())
            .build();

        kubernetesServer.expect().get().withPath("/api/v1/nodes/node")
            .andReturn(HttpURLConnection.HTTP_OK, node)
            .always();

        var props = DatabaseTestUtils.preparePostgresConfig("allocator", db.getConnectionInfo());
        // props.putAll(DatabaseTestUtils.prepareLocalhostConfig("allocator"));

        updateStartupProperties(props);

        allocatorCtx = ApplicationContext.run(props);
        ((MockKuberClientFactory) allocatorCtx.getBean(KuberClientFactory.class)).setClientSupplier(
            () -> kubernetesServer.createClient()
        );

        var config = allocatorCtx.getBean(ServiceConfig.class);
        config.getIam().setAddress("localhost:" + super.getPort());

        allocatorApp = allocatorCtx.getBean(AllocatorMain.class);
        allocatorApp.start();

        channel = newGrpcChannel(config.getAddress(), AllocatorGrpc.SERVICE_NAME, AllocatorPrivateGrpc.SERVICE_NAME,
            LongRunningServiceGrpc.SERVICE_NAME, DiskServiceGrpc.SERVICE_NAME);

        var credentials = config.getIam().createRenewableToken();
        unauthorizedAllocatorBlockingStub = AllocatorGrpc.newBlockingStub(channel);
        privateAllocatorBlockingStub = newBlockingClient(AllocatorPrivateGrpc.newBlockingStub(channel), "Test", null);
        operationServiceApiBlockingStub = newBlockingClient(LongRunningServiceGrpc.newBlockingStub(channel), "Test",
            () -> credentials.get().token());
        authorizedAllocatorBlockingStub = newBlockingClient(unauthorizedAllocatorBlockingStub, "Test",
            () -> credentials.get().token());
        diskService = newBlockingClient(DiskServiceGrpc.newBlockingStub(channel), "Test",
            () -> credentials.get().token());

        clusterRegistry = allocatorCtx.getBean(ClusterRegistry.class);
        operationsExecutor = allocatorCtx.getBean(OperationsExecutor.class,
            Qualifiers.byName("AllocatorOperationsExecutor"));

        metrics = allocatorCtx.getBean(AllocatorMetrics.class);
        gc = allocatorCtx.getBean(GarbageCollector.class);
        vmDao = allocatorCtx.getBean(VmDao.class);
        idGenerator = allocatorCtx.getBean(IdGenerator.class, Qualifiers.byName("AllocatorIdGenerator"));
    }

    protected void tearDown() {
        allocatorApp.stop(true);
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

        allocatorCtx.getBean(AllocatorDataSource.class).setOnClose(DatabaseTestUtils::cleanup);

        allocatorCtx.stop();
        kubernetesServer.destroy();
        super.after();
    }

    protected String createSession(Duration idleTimeout) {
        return createSession(idGenerator.generate("sid-"), idleTimeout);
    }

    protected String createSession(String owner, Duration idleTimeout) {
        var op = createSessionOp(owner, idleTimeout, null);
        return Utils.extractSessionId(op);
    }

    protected LongRunning.Operation deleteSession(String sessionId, boolean wait) {
        var op = withGrpcContext(() ->
            authorizedAllocatorBlockingStub.deleteSession(
                VmAllocatorApi.DeleteSessionRequest.newBuilder()
                    .setSessionId(sessionId)
                    .build()));

        if (wait) {
            op = waitOperation(operationServiceApiBlockingStub, op, 5);
        }

        return op;
    }

    protected LongRunning.Operation createSessionOp(String owner, Duration idleTimeout, @Nullable String token) {
        return withGrpcContext(() -> {
            var stub = authorizedAllocatorBlockingStub;

            if (token != null) {
                stub = withIdempotencyKey(stub, token);
            }

            var op = stub.createSession(
                VmAllocatorApi.CreateSessionRequest.newBuilder()
                    .setOwner(owner)
                    .setCachePolicy(
                        VmAllocatorApi.CachePolicy.newBuilder()
                            .setIdleTimeout(idleTimeout)
                            .build())
                    .build());
            Assert.assertTrue(op.getDone());
            return op;
        });
    }

    protected LongRunning.Operation waitOpSuccess(LongRunning.Operation operation) {
        var updatedOperation = waitOperation(operationServiceApiBlockingStub, operation, TIMEOUT_SEC);
        Assert.assertTrue(updatedOperation.hasResponse());
        Assert.assertFalse(updatedOperation.hasError());
        Assert.assertTrue(updatedOperation.getDone());
        return updatedOperation;
    }

    protected void waitOpError(LongRunning.Operation operation, Status expectedErrorStatus) {
        var updatedOperation = waitOperation(operationServiceApiBlockingStub, operation, TIMEOUT_SEC);
        Assert.assertFalse(updatedOperation.hasResponse());
        Assert.assertTrue(updatedOperation.hasError());
        Assert.assertEquals(expectedErrorStatus.getCode(),
            Status.fromCodeValue(updatedOperation.getError().getCode()).getCode());
    }

    protected void mockGetPodByName(String podName) {
        final Pod pod = constructPod(podName);
        kubernetesServer.expect().get()
            .withPath(POD_PATH + "/" + podName)
            .andReturn(HttpURLConnection.HTTP_OK, pod)
            .always();
    }

    @NotNull
    private static Pod constructPod(String podName) {
        final Pod pod = new Pod();
        pod.setMetadata(
            new ObjectMetaBuilder()
                .withName(podName)
                .withLabels(Map.of(
                    KuberLabels.LZY_VM_ID_LABEL, podName.substring(VM_POD_NAME_PREFIX.length())))
                .build()
        );
        pod.setSpec(new PodSpecBuilder()
            .withNodeName("node")
            .build());
        pod.setStatus(new PodStatusBuilder()
            .withPodIP("localhost")
            .build());
        return pod;
    }

    protected void registerVm(String vmId, String clusterId) {
        Vm vm;
        try {
            vm = vmDao.get(vmId, null);
        } catch (SQLException e) {
            Assert.fail(e.getMessage());
            return;
        }

        Assert.assertNotNull(vm);

        TimeUtils.waitFlagUp(() -> {
            try {
                return withGrpcContext(() -> {
                    privateAllocatorBlockingStub
                        .withInterceptors(OttHelper.createOttClientInterceptor(vmId, vm.allocateState().vmOtt()))
                        .register(
                            VmAllocatorPrivateApi.RegisterRequest.newBuilder()
                                .setVmId(vmId)
                                .putMetadata(NAMESPACE_KEY, NAMESPACE_VALUE)
                                .putMetadata(CLUSTER_ID_KEY, clusterId)
                                .build());
                    return true;
                });
            } catch (StatusRuntimeException e) {
                if (e.getStatus().getCode() == Status.Code.FAILED_PRECONDITION) {
                    LOG.error("Fail to register VM {}: {}", vmId, e.getStatus());
                    return false;
                }
                throw new RuntimeException(e);
            }
        }, TIMEOUT_SEC, TimeUnit.SECONDS);
    }

    protected <T> Future<T> awaitResourceCreate(Class<T> resourceType, String resourcePath) {
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

    protected Future<String> awaitAllocationRequest() {
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

    protected Future<String> awaitAllocationRequest(CountDownLatch latch) {
        final var future = new CompletableFuture<String>();
        kubernetesServer.expect().post()
            .withPath(POD_PATH)
            .andReply(HttpURLConnection.HTTP_CREATED, (req) -> {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                final var pod = Serialization.unmarshal(
                    new ByteArrayInputStream(req.getBody().readByteArray()), Pod.class, Map.of());
                future.complete(pod.getMetadata().getName());
                return pod;
            })
            .once();
        return future;
    }

    protected void mockDeleteResource(String resourcePath, String resourceName, Runnable onDelete,
                                                   int responseCode)
    {
        kubernetesServer.expect().delete()
            .withPath(resourcePath + "/" + resourceName)
            .andReply(responseCode, (req) -> {
                onDelete.run();
                return new StatusDetails();
            }).once();
    }

    protected void mockDeletePodByName(String podName, Runnable onDelete, int responseCode) {
        mockDeleteResource(POD_PATH, podName, onDelete, responseCode);
        kubernetesServer.expect().delete()
            // "lzy.ai/vm-id"=<VM id>
            .withPath(POD_PATH + "/" + podName)
            .andReply(responseCode, (req) -> {
                onDelete.run();
                return new StatusDetails();
            }).once();
    }

    protected record AllocatedVm(
        String vmId,
        String podName,
        String allocationOpId
    ) {}

    protected AllocatedVm allocateVm(String sessionId, @Nullable String idempotencyKey) throws Exception {
        return allocateVm(sessionId, "S", idempotencyKey);
    }

    protected AllocatedVm allocateVm(String sessionId, String pool, @Nullable String idempotencyKey) throws Exception {
        final var future = awaitAllocationRequest();

        var allocOp = withGrpcContext(() -> {
            var stub = authorizedAllocatorBlockingStub;
            if (idempotencyKey != null) {
                stub = withIdempotencyKey(stub, idempotencyKey);
            }

            return stub.allocate(
                VmAllocatorApi.AllocateRequest.newBuilder()
                    .setSessionId(sessionId)
                    .setPoolLabel(pool)
                    .setZone(ZONE)
                    .setClusterType(VmAllocatorApi.AllocateRequest.ClusterType.USER)
                    .addWorkload(VmAllocatorApi.AllocateRequest.Workload.getDefaultInstance())
                    .build());
        });

        if (allocOp.getDone()) {
            var vmId = allocOp.getMetadata().unpack(VmAllocatorApi.AllocateMetadata.class).getVmId();
            return new AllocatedVm(vmId, "unknown", allocOp.getId());
        }

        var vmId = allocOp.getMetadata().unpack(VmAllocatorApi.AllocateMetadata.class).getVmId();

        final String podName = future.get();
        mockGetPodByName(podName);

        String clusterId = withGrpcContext(() ->
            requireNonNull(clusterRegistry.findCluster(pool, ZONE, CLUSTER_TYPE)).clusterId());
        registerVm(vmId, clusterId);

        allocOp = waitOpSuccess(allocOp);
        Assert.assertEquals(vmId, allocOp.getResponse().unpack(VmAllocatorApi.AllocateResponse.class).getVmId());

        return new AllocatedVm(vmId, podName, allocOp.getId());
    }

    protected void freeVm(String vmId) {
        withGrpcContext(() ->
            authorizedAllocatorBlockingStub.free(VmAllocatorApi.FreeRequest.newBuilder().setVmId(vmId).build()));
    }

    protected void assertVmMetrics(String pool, int allocating, int running, int cached) {
        if (allocating >= 0) {
            Assert.assertEquals(allocating, (int) metrics.runningAllocations.labels(pool).get());
        }
        if (running >= 0) {
            Assert.assertEquals(running, (int) metrics.runningVms.labels(pool).get());
        }
        if (cached >= 0) {
            Assert.assertEquals(cached, (int) metrics.cachedVms.labels(pool).get());
        }
    }

    @NotNull
    public static String getVmPodName(String vmId) {
        return KuberVmAllocator.VM_POD_NAME_PREFIX + vmId.toLowerCase(Locale.ROOT);
    }

    @NotNull
    public static String getTunnelPodName(String vmId) {
        return KuberTunnelAllocator.TUNNEL_POD_NAME_PREFIX + vmId.toLowerCase(Locale.ROOT);
    }
}
