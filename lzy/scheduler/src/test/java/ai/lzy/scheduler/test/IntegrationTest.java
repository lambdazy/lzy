package ai.lzy.scheduler.test;

import ai.lzy.iam.test.BaseTestWithIam;
import ai.lzy.model.utils.FreePortFinder;
import ai.lzy.scheduler.SchedulerApi;
import ai.lzy.scheduler.configs.ServiceConfig;
import ai.lzy.scheduler.test.mocks.AllocatedWorkerMock;
import ai.lzy.scheduler.test.mocks.AllocatorMock;
import ai.lzy.util.grpc.GrpcChannels;
import ai.lzy.v1.common.LME;
import ai.lzy.v1.common.LMO;
import ai.lzy.v1.scheduler.SchedulerApi.TaskScheduleRequest;
import ai.lzy.v1.scheduler.SchedulerGrpc;
import ai.lzy.v1.scheduler.SchedulerPrivateGrpc;
import io.grpc.ManagedChannel;
import io.micronaut.context.ApplicationContext;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.*;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;

import static ai.lzy.model.db.test.DatabaseTestUtils.preparePostgresConfig;
import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;

public class IntegrationTest extends BaseTestWithIam {

    @Rule
    public PreparedDbRule iamDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});
    @Rule
    public PreparedDbRule db = EmbeddedPostgresRules.preparedDatabase(ds -> {});

    private ApplicationContext context;
    private SchedulerApi api;
    private SchedulerGrpc.SchedulerBlockingStub stub;
    ManagedChannel privateChan;
    ManagedChannel chan;

    @Before
    public void setUp() throws IOException {
        super.setUp(preparePostgresConfig("iam", iamDb.getConnectionInfo()));

        var props = preparePostgresConfig("scheduler", db.getConnectionInfo());
        props.putAll(preparePostgresConfig("jobs", db.getConnectionInfo()));

        context = ApplicationContext.run(props);
        ServiceConfig config = context.getBean(ServiceConfig.class);
        api = context.getBean(SchedulerApi.class);
        var auth = config.getIam();

        var credentials = auth.createRenewableToken();

        chan = newGrpcChannel("localhost", 2392, SchedulerGrpc.SERVICE_NAME);
        stub = newBlockingClient(SchedulerGrpc.newBlockingStub(chan), "Test", () -> credentials.get().token());

        privateChan = newGrpcChannel("localhost", 2392, SchedulerPrivateGrpc.SERVICE_NAME);

        Configurator.setAllLevels("ai.lzy.scheduler", Level.ALL);
    }

    @After
    public void tearDown() throws InterruptedException {
        api.close();
        api.awaitTermination();
        GrpcChannels.awaitTermination(privateChan, Duration.ofSeconds(15), getClass());
        GrpcChannels.awaitTermination(chan, Duration.ofSeconds(15), getClass());
        context.close();
    }

    @Test
    public void testSimple() throws Exception {
        var allocate = new CountDownLatch(1);
        var latch = new CountDownLatch(1);
        var exec = new CountDownLatch(1);

        final var port = FreePortFinder.find(1000, 2000);

        final var worker = new AllocatedWorkerMock(port, (a) -> {
            exec.countDown();
            return true;
        });

        AllocatorMock.onAllocate = (a, b, c) -> {
            allocate.countDown();
            return "localhost:" + port;
        };

        AllocatorMock.onDestroy = (a) -> latch.countDown();

        var resp = stub.schedule(TaskScheduleRequest.newBuilder()
            .setWorkflowId("wfid")
            .setWorkflowName("wf")
            .setUserId("uid")
            .setTask(LMO.TaskDesc.newBuilder()
                .setOperation(LMO.Operation.newBuilder()
                    .setName("name")
                    .setRequirements(LMO.Requirements.newBuilder()
                        .setPoolLabel("s")
                        .setZone("a").build())
                    .setCommand("")
                    .setDescription("")
                    .setEnv(LME.EnvSpec.newBuilder().build())
                    .build())
                .build())
            .build());

        allocate.await();
        exec.await();
        latch.await();

        var r = stub.status(ai.lzy.v1.scheduler.SchedulerApi.TaskStatusRequest.newBuilder()
            .setTaskId(resp.getStatus().getTaskId())
            .setWorkflowId(resp.getStatus().getWorkflowId())
            .build());

        Assert.assertTrue(r.getStatus().hasSuccess());
    }
}
