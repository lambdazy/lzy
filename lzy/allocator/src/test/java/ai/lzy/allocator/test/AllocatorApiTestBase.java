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
import ai.lzy.allocator.test.http.MockHttpDispatcher;
import ai.lzy.allocator.vmpool.ClusterRegistry;
import ai.lzy.common.IdGenerator;
import ai.lzy.iam.test.BaseTestWithIam;
import ai.lzy.longrunning.OperationsExecutor;
import ai.lzy.model.db.test.DatabaseTestUtils;
import ai.lzy.test.TimeUtils;
import ai.lzy.util.auth.credentials.OttHelper;
import ai.lzy.v1.*;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Duration;
import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.http.TlsVersion;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.micronaut.context.ApplicationContext;
import io.micronaut.inject.qualifiers.Qualifiers;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okio.Buffer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Rule;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static ai.lzy.allocator.alloc.impl.kuber.KuberVmAllocator.*;
import static ai.lzy.allocator.test.Utils.waitOperation;
import static ai.lzy.allocator.test.http.RequestMatchers.*;
import static ai.lzy.test.GrpcUtils.withGrpcContext;
import static ai.lzy.util.grpc.GrpcUtils.*;
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
    protected ManagedChannel channel;
    protected ClusterRegistry clusterRegistry;
    protected OperationsExecutor operationsExecutor;
    protected AllocatorMetrics metrics;
    protected GarbageCollector gc;
    protected VmDao vmDao;
    protected IdGenerator idGenerator;
    protected ObjectMapper objectMapper;
    protected MockWebServer mockWebServer;
    protected MockHttpDispatcher mockRequestDispatcher;

    protected void updateStartupProperties(Map<String, Object> props) {}

    protected void setUp() throws IOException {
        super.setUp(DatabaseTestUtils.preparePostgresConfig("iam", iamDb.getConnectionInfo()));

        objectMapper = new ObjectMapper();
        mockWebServer = new MockWebServer();
        this.mockRequestDispatcher = new MockHttpDispatcher();
        mockWebServer.setDispatcher(mockRequestDispatcher);
        mockWebServer.start();

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

        mockRequestDispatcher.addHandlerUnlimited(exactPath("/api/v1/nodes/node").and(method("GET")), req -> new MockResponse()
            .setResponseCode(HttpURLConnection.HTTP_OK)
            .setBody(toJson(node)));

        var props = DatabaseTestUtils.preparePostgresConfig("allocator", db.getConnectionInfo());
        // props.putAll(DatabaseTestUtils.prepareLocalhostConfig("allocator"));

        updateStartupProperties(props);

        allocatorCtx = ApplicationContext.run(props);
        ((MockKuberClientFactory) allocatorCtx.getBean(KuberClientFactory.class)).setClientSupplier(
            () -> prepareClient(mockWebServer)
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

    private static KubernetesClient prepareClient(MockWebServer server) {
        return new KubernetesClientBuilder()
            .withConfig(new io.fabric8.kubernetes.client.ConfigBuilder(Config.empty())
                .withMasterUrl("http://localhost:" + server.getPort())
                .withTrustCerts(true)
                .withTlsVersions(TlsVersion.TLS_1_2)
                .withNamespace("test")
                .withHttp2Disable(true)
                .build())
            .build();
    }

    protected String toJson(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    protected  <T> T fromJson(String string, Class<T> valueType) {
        try {
            return objectMapper.readValue(string, valueType);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected  <T> T fromJson(Buffer buffer, Class<T> valueType) {
        try {
            return objectMapper.readValue(buffer.readUtf8(), valueType);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
        try {
            mockWebServer.shutdown();
        } catch (IOException e) {
            LOG.error("Failed to shutdown mockWebServer", e);
        }
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
        mockRequestDispatcher.addHandlerOneTime(exactPath(POD_PATH + "/" + podName).and(method("GET")),
            request -> new MockResponse().setBody(toJson(pod)).setResponseCode(HttpURLConnection.HTTP_OK));
    }

    protected void mockGetPod(Pod pod) {
        mockRequestDispatcher.addHandlerOneTime(exactPath(POD_PATH + "/" + getName(pod)).and(method("GET")),
            request -> new MockResponse().setBody(toJson(pod)).setResponseCode(HttpURLConnection.HTTP_OK));
    }

    @Nonnull
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
        return awaitResourceCreate(resourceType, resourcePath, HttpURLConnection.HTTP_CREATED);
    }

    protected <T> Future<T> awaitResourceCreate(Class<T> resourceType, String resourcePath, int statusCode) {
        final var future = new CompletableFuture<T>();
        mockRequestDispatcher.addHandlerOneTime(exactPath(resourcePath).and(method("POST")),
            request -> {
                var resource = fromJson(request.getBody(), resourceType);
                future.complete(resource);
                return new MockResponse().setBody(toJson(resource)).setResponseCode(statusCode);
            });
        return future;
    }

    protected CompletableFuture<Pod> mockCreatePod(@Nullable Consumer<String> onAllocate) {
        final var future = new CompletableFuture<Pod>();
        mockRequestDispatcher.addHandlerOneTime(exactPath(POD_PATH).and(method("POST")),
            request -> {
                var pod = fromJson(request.getBody(), Pod.class);

                if (onAllocate != null) {
                    onAllocate.accept(pod.getMetadata().getName());
                }

                future.complete(pod);
                return new MockResponse().setBody(request.getBody()).setResponseCode(HttpURLConnection.HTTP_CREATED);
            });
        return future;
    }

    protected CompletableFuture<Pod> mockCreatePod() {
        return mockCreatePod(null);
    }

    protected void mockDeleteResource(String resourcePath, String resourceName, Runnable onDelete, int responseCode) {
        mockRequestDispatcher.addHandlerOneTime(exactPath(resourcePath + "/" + resourceName).and(method("DELETE")),
            request -> {
                onDelete.run();
                return new MockResponse().setBody(toJson(new StatusDetails())).setResponseCode(responseCode);
            });
    }

    protected void mockDeletePodByName(String podName, int responseCode) {
        mockDeletePodByName(podName, () -> {}, responseCode);
    }

    protected void mockDeletePodByName(String podName, Runnable onDelete, int responseCode) {
        mockDeleteResource(POD_PATH, podName, onDelete, responseCode);
    }

    protected void mockDeletePods(int responseCode) {
        mockDeletePods(responseCode, () -> {});
    }

    protected void mockDeletePods(int responseCode, Runnable onDelete) {
        mockRequestDispatcher.addHandlerOneTime(startsWithPath(POD_PATH).and(method("DELETE")),
            request ->  {
                onDelete.run();
                return new MockResponse().setBody(toJson(new StatusDetails())).setResponseCode(responseCode);
            });
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
        final var future = mockCreatePod(this::mockGetPodByName);

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

        final String podName = getName(future.get());

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

    @Nonnull
    public static String getVmPodName(String vmId) {
        return KuberVmAllocator.VM_POD_NAME_PREFIX + vmId.toLowerCase(Locale.ROOT);
    }

    @Nonnull
    public static String getTunnelPodName(String vmId) {
        return KuberTunnelAllocator.TUNNEL_POD_NAME_PREFIX + vmId.toLowerCase(Locale.ROOT);
    }

    @Nonnull
    public static String getName(HasMetadata resource) {
        return resource.getMetadata().getName();
    }
}
