package ai.lzy.allocator.test;

import ai.lzy.allocator.AllocatorMain;
import ai.lzy.allocator.alloc.impl.kuber.KuberClientFactory;
import ai.lzy.allocator.alloc.impl.kuber.KuberLabels;
import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.storage.AllocatorDataSource;
import ai.lzy.allocator.vmpool.ClusterRegistry;
import ai.lzy.iam.test.BaseTestWithIam;
import ai.lzy.model.db.test.DatabaseTestUtils;
import ai.lzy.test.TimeUtils;
import ai.lzy.v1.AllocatorGrpc;
import ai.lzy.v1.AllocatorPrivateGrpc;
import ai.lzy.v1.DiskServiceGrpc;
import ai.lzy.v1.VmAllocatorApi;
import ai.lzy.v1.VmAllocatorPrivateApi;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import com.google.protobuf.Duration;
import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.client.server.mock.KubernetesServer;
import io.fabric8.kubernetes.client.utils.Serialization;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.micronaut.context.ApplicationContext;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import org.junit.Assert;
import org.junit.Rule;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

import static ai.lzy.allocator.alloc.impl.kuber.KuberVmAllocator.*;
import static ai.lzy.allocator.test.Utils.waitOperation;
import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;
import static ai.lzy.util.grpc.GrpcUtils.withIdempotencyKey;

public class AllocatorApiTestBase extends BaseTestWithIam {

    protected static final long TIMEOUT_SEC = 10;

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
    protected KubernetesServer kubernetesServer;
    protected ManagedChannel channel;
    protected ClusterRegistry clusterRegistry;

    protected void updateStartupProperties(Map<String, Object> props) {}

    protected void setUp() throws IOException {
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

        updateStartupProperties(props);

        allocatorCtx = ApplicationContext.run(props);
        ((MockKuberClientFactory) allocatorCtx.getBean(KuberClientFactory.class)).setClientSupplier(
            () -> kubernetesServer.getKubernetesMockServer().createClient()
        );

        var config = allocatorCtx.getBean(ServiceConfig.class);
        config.getIam().setAddress("localhost:" + super.getPort());

        allocatorApp = allocatorCtx.getBean(AllocatorMain.class);
        allocatorApp.start();

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

    protected void tearDown() {
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

        allocatorCtx.getBean(AllocatorDataSource.class).setOnClose(DatabaseTestUtils::cleanup);

        allocatorCtx.stop();
        kubernetesServer.after();
        super.after();
    }

    protected String createSession(Duration idleTimeout) {
        var op = createSessionOp(UUID.randomUUID().toString(), idleTimeout, null);
        return Utils.extractSessionId(op);
    }

    protected LongRunning.Operation createSessionOp(String owner, Duration idleTimeout, @Nullable String token) {
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
        Assert.assertEquals(expectedErrorStatus.getCode().value(), updatedOperation.getError().getCode());
    }

    protected void mockGetPod(String podName) {
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
        kubernetesServer.expect().get()
            .withPath(POD_PATH + "?labelSelector=" +
                URLEncoder.encode(KuberLabels.LZY_POD_NAME_LABEL + "=" + podName, StandardCharsets.UTF_8))
            .andReturn(HttpURLConnection.HTTP_OK, new PodListBuilder().withItems(pod).build())
            .always();
    }

    protected void registerVm(String vmId, String clusterId) {
        TimeUtils.waitFlagUp(() -> {
            try {
                //noinspection ResultOfMethodCallIgnored
                privateAllocatorBlockingStub.register(
                    VmAllocatorPrivateApi.RegisterRequest.newBuilder()
                        .setVmId(vmId)
                        .putMetadata(NAMESPACE_KEY, NAMESPACE_VALUE)
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

    protected void mockDeleteResource(String resourcePath, String resourceName, Runnable onDelete, int responseCode) {
        kubernetesServer.expect().delete()
            .withPath(resourcePath + "/" + resourceName)
            .andReply(responseCode, (req) -> {
                onDelete.run();
                return new StatusDetails();
            }).once();
    }

    protected void mockDeletePod(String podName, Runnable onDelete, int responseCode) {
        mockDeleteResource(POD_PATH, podName, onDelete, responseCode);
        kubernetesServer.expect().delete()
            // "lzy.ai/vm-id"=<VM id>
            .withPath(POD_PATH + "?labelSelector=lzy.ai%2Fvm-id%3D" + podName.substring(VM_POD_NAME_PREFIX.length()))
            .andReply(responseCode, (req) -> {
                onDelete.run();
                return new StatusDetails();
            }).once();
    }
}
