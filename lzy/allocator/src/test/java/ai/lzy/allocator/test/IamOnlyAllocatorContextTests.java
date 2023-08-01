package ai.lzy.allocator.test;

import ai.lzy.allocator.alloc.AllocatorMetrics;
import ai.lzy.allocator.alloc.dao.VmDao;
import ai.lzy.allocator.gc.GarbageCollector;
import ai.lzy.allocator.vmpool.ClusterRegistry;
import ai.lzy.common.IdGenerator;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.iam.test.IamContextImpl;
import ai.lzy.longrunning.OperationsExecutor;
import ai.lzy.test.context.LzyInThread;
import ai.lzy.test.context.config.LzyConfig;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.v1.AllocatorGrpc;
import ai.lzy.v1.AllocatorPrivateGrpc;
import ai.lzy.v1.DiskServiceGrpc;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import io.grpc.ManagedChannel;
import io.micronaut.context.ApplicationContext;
import io.micronaut.inject.qualifiers.Qualifiers;
import io.zonky.test.db.postgres.embedded.ConnectionInfo;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;

public abstract class IamOnlyAllocatorContextTests {
    @Rule
    public PreparedDbRule iamDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});
    @Rule
    public PreparedDbRule allocatorDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});

    public LzyInThread lzy = new LzyInThread();

    protected ApplicationContext allocatorContext;

    protected AllocatorGrpc.AllocatorBlockingStub unauthorizedAllocatorBlockingStub;
    protected AllocatorGrpc.AllocatorBlockingStub authorizedAllocatorBlockingStub;
    protected AllocatorPrivateGrpc.AllocatorPrivateBlockingStub privateAllocatorBlockingStub;
    protected LongRunningServiceGrpc.LongRunningServiceBlockingStub operationServiceApiBlockingStub;
    protected DiskServiceGrpc.DiskServiceBlockingStub diskServiceBlockingStub;
    protected ManagedChannel allocatorChannel;
    protected ClusterRegistry clusterRegistry;
    protected AllocatorMetrics metrics;
    protected VmDao vmDao;
    protected IdGenerator idGenerator;
    protected GarbageCollector gc;
    protected OperationsExecutor operationsExecutor;
    protected RenewableJwt internalUserCredentials;

    @Before
    public final void setUp() throws InterruptedException {
        var configs = LzyConfig.Configs.builder()
            .setIamConfig("../iam/src/main/resources/application-test.yml")
            .setAllocatorConfig("../allocator/src/main/resources/application-test.yml")
            .build();

        var environments = LzyConfig.Environments.builder()
            .addIamEnvironment(ai.lzy.iam.BeanFactory.TEST_ENV_NAME)
            .addAllocatorEnvironment("test")
            .build();

        var ports = LzyConfig.Ports.findFree();

        var database = LzyConfig.Database.builder()
            .setIamDbUrl(prepareDbUrl(iamDb.getConnectionInfo()))
            .setAllocatorDbUrl(prepareDbUrl(allocatorDb.getConnectionInfo()))
            .build();

        lzy.setUp(configs, allocatorConfigOverrides(), environments, ports, database, AllocatorContextImpl.ENV_NAME,
            IamContextImpl.ENV_NAME);

        internalUserCredentials = lzy.micronautContext().getBean(IamContextImpl.class).clientConfig()
            .createRenewableToken();

        allocatorChannel = newGrpcChannel("localhost:" + ports.getAllocatorPort(), AllocatorGrpc.SERVICE_NAME,
            AllocatorPrivateGrpc.SERVICE_NAME, LongRunningServiceGrpc.SERVICE_NAME, DiskServiceGrpc.SERVICE_NAME);
        unauthorizedAllocatorBlockingStub = AllocatorGrpc.newBlockingStub(allocatorChannel);
        privateAllocatorBlockingStub = newBlockingClient(AllocatorPrivateGrpc.newBlockingStub(allocatorChannel),
            "Test", null);
        operationServiceApiBlockingStub = newBlockingClient(LongRunningServiceGrpc.newBlockingStub(allocatorChannel),
            "Test", () -> internalUserCredentials.get().token());
        authorizedAllocatorBlockingStub = newBlockingClient(unauthorizedAllocatorBlockingStub, "Test",
            () -> internalUserCredentials.get().token());
        diskServiceBlockingStub = newBlockingClient(DiskServiceGrpc.newBlockingStub(allocatorChannel), "Test",
            () -> internalUserCredentials.get().token());

        allocatorContext = lzy.micronautContext().getBean(AllocatorContextImpl.class).getMicronautContext();

        clusterRegistry = allocatorContext.getBean(ClusterRegistry.class);
        metrics = allocatorContext.getBean(AllocatorMetrics.class);
        vmDao = allocatorContext.getBean(VmDao.class);
        idGenerator = allocatorContext.getBean(IdGenerator.class, Qualifiers.byName("AllocatorIdGenerator"));
        gc = allocatorContext.getBean(GarbageCollector.class);
        operationsExecutor = allocatorContext.getBean(OperationsExecutor.class,
            Qualifiers.byName("AllocatorOperationsExecutor"));
    }

    @After
    public void tearDown() {
        allocatorChannel.shutdown();
        try {
            allocatorChannel.awaitTermination(60, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
            //ignored
        } finally {
            lzy.tearDown();
        }
    }

    public Subject createAdminSubject(String name, String publicKey) {
        return lzy.micronautContext().getBean(IamContextImpl.class).createAdminSubject(name, publicKey);
    }

    protected abstract Map<String, Object> allocatorConfigOverrides();

    public static String prepareDbUrl(ConnectionInfo ci) {
        return "jdbc:postgresql://localhost:%d/%s".formatted(ci.getPort(), ci.getDbName());
    }
}
