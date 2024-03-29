package ai.lzy.allocator.test;

import ai.lzy.allocator.alloc.impl.kuber.KuberClientFactory;
import ai.lzy.allocator.alloc.impl.kuber.KuberLabels;
import ai.lzy.allocator.alloc.impl.kuber.KuberTunnelAllocator;
import ai.lzy.allocator.alloc.impl.kuber.KuberVmAllocator;
import ai.lzy.allocator.model.PersistentVolumePhase;
import ai.lzy.allocator.model.Vm;
import ai.lzy.allocator.test.http.MockHttpDispatcher;
import ai.lzy.allocator.vmpool.ClusterRegistry;
import ai.lzy.test.TimeUtils;
import ai.lzy.util.auth.credentials.OttHelper;
import ai.lzy.v1.VmAllocatorApi;
import ai.lzy.v1.VmAllocatorPrivateApi;
import ai.lzy.v1.longrunning.LongRunning;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Duration;
import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.http.TlsVersion;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import okhttp3.Response;
import okhttp3.WebSocket;
import okhttp3.WebSocketListener;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okio.Buffer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

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
import static ai.lzy.allocator.test.http.RequestMatchers.exactPath;
import static ai.lzy.allocator.test.http.RequestMatchers.method;
import static ai.lzy.allocator.test.http.RequestMatchers.startsWithPath;
import static ai.lzy.test.GrpcUtils.withGrpcContext;
import static ai.lzy.util.grpc.GrpcUtils.withIdempotencyKey;
import static java.util.Objects.requireNonNull;

public abstract class AllocatorApiTestBase extends IamOnlyAllocatorContextTests {
    private static final Logger LOG = LogManager.getLogger(AllocatorApiTestBase.class);

    protected static final long TIMEOUT_SEC = 10;
    protected static final String ZONE = "test-zone";

    protected static final String POD_PATH = "/api/v1/namespaces/%s/pods".formatted(NAMESPACE_VALUE);
    protected static final String PERSISTENT_VOLUME_PATH = "/api/v1/persistentvolumes";
    protected static final String PERSISTENT_VOLUME_CLAIM_PATH = "/api/v1/namespaces/%s/persistentvolumeclaims"
        .formatted(NAMESPACE_VALUE);
    protected static final ClusterRegistry.ClusterType CLUSTER_TYPE = ClusterRegistry.ClusterType.User;

    protected ObjectMapper objectMapper;
    protected MockWebServer mockWebServer;
    protected MockHttpDispatcher mockRequestDispatcher;

    @Before
    public final void setUpKuberServer() throws IOException {
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

        mockRequestDispatcher.addHandlerUnlimited(exactPath("/api/v1/nodes/node").and(method("GET")),
            req -> new MockResponse()
                .setResponseCode(HttpURLConnection.HTTP_OK)
                .setBody(toJson(node)));

        ((MockKuberClientFactory) allocatorContext.getBean(KuberClientFactory.class)).setClientSupplier(
            () -> prepareClient(mockWebServer));
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

    protected <T> T fromJson(String string, Class<T> valueType) {
        try {
            return objectMapper.readValue(string, valueType);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected <T> T fromJson(Buffer buffer, Class<T> valueType) {
        try {
            return objectMapper.readValue(buffer.readUtf8(), valueType);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @After
    @Override
    public void tearDown() {
        super.tearDown();
        try {
            mockWebServer.shutdown();
        } catch (IOException e) {
            LOG.error("Failed to shutdown mockWebServer", e);
        }
    }

    protected String createSession(com.google.protobuf.Duration idleTimeout) {
        return createSession(idGenerator.generate("sid-"), idleTimeout);
    }

    protected String createSession(String owner, com.google.protobuf.Duration idleTimeout) {
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
        return waitOpSuccess(operation.getId());
    }

    protected LongRunning.Operation waitOpSuccess(String operationId) {
        var updatedOperation = waitOperation(operationServiceApiBlockingStub, operationId, TIMEOUT_SEC);
        Assert.assertTrue(updatedOperation.hasResponse());
        Assert.assertFalse(updatedOperation.hasError());
        Assert.assertTrue(updatedOperation.getDone());
        return updatedOperation;
    }

    protected void waitOpError(LongRunning.Operation operation, io.grpc.Status expectedErrorStatus) {
        var updatedOperation = waitOperation(operationServiceApiBlockingStub, operation, TIMEOUT_SEC);
        Assert.assertFalse(updatedOperation.hasResponse());
        Assert.assertTrue(updatedOperation.hasError());
        Assert.assertEquals(expectedErrorStatus.getCode(),
            io.grpc.Status.fromCodeValue(updatedOperation.getError().getCode()).getCode());
    }

    protected void mockGetPodByName(String podName) {
        final Pod pod = constructPod(podName);
        mockRequestDispatcher.addHandlerOneTime(
            exactPath(POD_PATH + "/" + podName).and(method("GET")),
            request -> new MockResponse().setBody(toJson(pod)).setResponseCode(HttpURLConnection.HTTP_OK));
    }

    protected void mockGetPod(Pod pod) {
        mockRequestDispatcher.addHandlerOneTime(
            exactPath(POD_PATH + "/" + getName(pod)).and(method("GET")),
            request -> new MockResponse().setBody(toJson(pod)).setResponseCode(HttpURLConnection.HTTP_OK));
    }

    protected void mockGetPodNotFound(String podName) {
        mockRequestDispatcher.addHandlerOneTime(
            exactPath(POD_PATH + "/" + podName).and(method("GET")),
            request -> new MockResponse().setResponseCode(HttpURLConnection.HTTP_NOT_FOUND));
    }

    protected void mockGetPv(PersistentVolume pv) {
        mockRequestDispatcher.addHandlerOneTime(
            exactPath(PERSISTENT_VOLUME_PATH + "/" + getName(pv)).and(method("GET")),
            request -> new MockResponse().setBody(toJson(pv)).setResponseCode(HttpURLConnection.HTTP_OK));
    }

    protected void mockGetPvNotFound(String pvName) {
        mockRequestDispatcher.addHandlerOneTime(
            exactPath(PERSISTENT_VOLUME_PATH + "/" + pvName).and(method("GET")),
            request -> new MockResponse().setResponseCode(HttpURLConnection.HTTP_NOT_FOUND));
    }

    protected void mockExecInPod(Pod pod) {
        mockRequestDispatcher.addHandlerUnlimited(
            startsWithPath(POD_PATH + "/" + getName(pod) + "/exec").and(method("GET")),
            request -> new MockResponse().withWebSocketUpgrade(new WebSocketListener() {
                @Override
                public void onOpen(@Nonnull WebSocket webSocket, @Nonnull Response response) {
                    super.onOpen(webSocket, response);
                    webSocket.close(1000, "");
                }
            }));
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

    protected <T> CompletableFuture<T> awaitResourceCreate(Class<T> resourceType, String resourcePath, int statusCode) {
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
                return new MockResponse().setBody(toJson(pod)).setResponseCode(HttpURLConnection.HTTP_CREATED);
            });
        return future;
    }

    protected CompletableFuture<Pod> mockCreatePod() {
        return mockCreatePod(null);
    }

    protected CompletableFuture<PersistentVolume> mockCreatePv() {
        final var future = new CompletableFuture<PersistentVolume>();
        mockRequestDispatcher.addHandlerOneTime(exactPath(PERSISTENT_VOLUME_PATH).and(method("POST")),
            request -> {
                var pv = fromJson(request.getBody(), PersistentVolume.class);
                pv.setStatus(new PersistentVolumeStatusBuilder()
                        .withMessage("Ok")
                        .withPhase(PersistentVolumePhase.AVAILABLE.getPhase())
                    .build());
                future.complete(pv);
                return new MockResponse().setBody(toJson(pv)).setResponseCode(HttpURLConnection.HTTP_CREATED);
            });
        return future;
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
            request -> {
                onDelete.run();
                return new MockResponse().setBody(toJson(new StatusDetails())).setResponseCode(responseCode);
            });
    }

    protected record AllocatedVm(String vmId, String podName, String allocationOpId) {}

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
            authorizedAllocatorBlockingStub.free(VmAllocatorApi.FreeRequest.newBuilder()
                .setVmId(vmId)
                .build()));
    }

    protected LongRunning.Operation forceFreeVm(String vmId) {
        return withGrpcContext(() ->
            authorizedAllocatorBlockingStub.forceFree(VmAllocatorApi.ForceFreeRequest.newBuilder()
                .setVmId(vmId)
                .build()));
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

    protected void assertVmMetrics(String pool, int allocating, int running, int cached, java.time.Duration timeout) {
        TimeUtils.waitFlagUp(() -> {
            try {
                assertVmMetrics(pool, allocating, running, cached);
                return true;
            } catch (AssertionError ignored) {
                return false;
            }
        }, timeout.toMillis(), TimeUnit.MILLISECONDS);
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

    @Override
    protected Map<String, Object> allocatorConfigOverrides() {
        return Map.of();
    }
}
