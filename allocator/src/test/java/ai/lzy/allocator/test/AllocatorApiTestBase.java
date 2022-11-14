package ai.lzy.allocator.test;

import ai.lzy.allocator.AllocatorMain;
import ai.lzy.allocator.alloc.impl.kuber.KuberClientFactory;
import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.dao.impl.AllocatorDataSource;
import ai.lzy.allocator.vmpool.ClusterRegistry;
import ai.lzy.iam.test.BaseTestWithIam;
import ai.lzy.model.db.test.DatabaseTestUtils;
import ai.lzy.v1.AllocatorGrpc;
import ai.lzy.v1.AllocatorPrivateGrpc;
import ai.lzy.v1.DiskServiceGrpc;
import ai.lzy.v1.VmAllocatorApi;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import com.google.protobuf.Duration;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.NodeAddressBuilder;
import io.fabric8.kubernetes.api.model.NodeStatusBuilder;
import io.fabric8.kubernetes.api.model.PodListBuilder;
import io.fabric8.kubernetes.client.server.mock.KubernetesServer;
import io.grpc.ManagedChannel;
import io.micronaut.context.ApplicationContext;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import org.junit.Assert;
import org.junit.Rule;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;
import static ai.lzy.util.grpc.GrpcUtils.withIdempotencyKey;

public class AllocatorApiTestBase extends BaseTestWithIam {

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

        DatabaseTestUtils.cleanup(allocatorCtx.getBean(AllocatorDataSource.class));

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
}
