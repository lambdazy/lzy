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
import ai.lzy.v1.scheduler.Scheduler;
import ai.lzy.v1.scheduler.SchedulerApi.TaskScheduleRequest;
import ai.lzy.v1.scheduler.SchedulerGrpc;
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
    ManagedChannel chan;

    @Before
    public void setUp() throws IOException {
        super.setUp(preparePostgresConfig("iam", iamDb.getConnectionInfo()));

        var props = preparePostgresConfig("scheduler", db.getConnectionInfo());
        props.putAll(preparePostgresConfig("jobs", db.getConnectionInfo()));

        context = ApplicationContext.run(props);

        var config = context.getBean(ServiceConfig.class);
        config.getIam().setAddress("localhost:" + super.getPort());

        api = context.getBean(SchedulerApi.class);
        var auth = config.getIam();

        var credentials = auth.createRenewableToken();

        chan = newGrpcChannel("localhost", 2392, SchedulerGrpc.SERVICE_NAME);
        stub = newBlockingClient(SchedulerGrpc.newBlockingStub(chan), "Test", () -> credentials.get().token());

        Configurator.setAllLevels("ai.lzy.scheduler", Level.ALL);
    }

    @After
    public void tearDown() throws InterruptedException {
        api.close();
        api.awaitTermination();
        GrpcChannels.awaitTermination(chan, Duration.ofSeconds(15), getClass());
        context.close();
        super.after();
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

    @Test
    public void testParallel() throws Exception {
        var allocate = new CountDownLatch(2);
        var latch = new CountDownLatch(2);
        var exec = new CountDownLatch(2);

        final var port1 = FreePortFinder.find(1000, 2000);

        final var worker1 = new AllocatedWorkerMock(port1, (a) -> {
            exec.countDown();
            return true;
        });

        final var port2 = FreePortFinder.find(1000, 2000);

        final var worker2 = new AllocatedWorkerMock(port2, (a) -> {
            exec.countDown();
            return true;
        });

        AllocatorMock.onAllocate = (a, b, c) -> {
            allocate.countDown();

            AllocatorMock.onAllocate = (a1, b1, c1) -> {
                allocate.countDown();

                return "localhost:" + port2;
            };
            return "localhost:" + port1;
        };

        AllocatorMock.onDestroy = (a) -> latch.countDown();

        var resp1 = stub.schedule(TaskScheduleRequest.newBuilder()
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

        var resp2 = stub.schedule(TaskScheduleRequest.newBuilder()
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

        var r1 = stub.status(ai.lzy.v1.scheduler.SchedulerApi.TaskStatusRequest.newBuilder()
            .setTaskId(resp1.getStatus().getTaskId())
            .setWorkflowId(resp1.getStatus().getWorkflowId())
            .build());

        var r2 = stub.status(ai.lzy.v1.scheduler.SchedulerApi.TaskStatusRequest.newBuilder()
            .setTaskId(resp2.getStatus().getTaskId())
            .setWorkflowId(resp2.getStatus().getWorkflowId())
            .build());

        Assert.assertTrue(r1.getStatus().hasSuccess());
        Assert.assertTrue(r2.getStatus().hasSuccess());
    }

    @Test
    public void testFailExec() throws Exception {
        var allocate = new CountDownLatch(1);
        var latch = new CountDownLatch(1);
        var exec = new CountDownLatch(1);

        final var port = FreePortFinder.find(1000, 2000);

        final var worker = new AllocatedWorkerMock(port, (a) -> {
            exec.countDown();
            return false;
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

        Assert.assertTrue(r.getStatus().hasError());
    }

    @Test
    public void testFailAllocate() throws Exception {
        var allocate = new CountDownLatch(1);

        AllocatorMock.onAllocate = (a, b, c) -> {
            allocate.countDown();
            throw new RuntimeException("");
        };

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

        Scheduler.TaskStatus taskStatus = null;

        var retries = 0;
        while ((++retries) < 10) {   // Waiting for task fail
            var r = stub.status(ai.lzy.v1.scheduler.SchedulerApi.TaskStatusRequest.newBuilder()
                .setTaskId(resp.getStatus().getTaskId())
                .setWorkflowId(resp.getStatus().getWorkflowId())
                .build());

            if (!r.getStatus().hasExecuting()){
                taskStatus = r.getStatus();
                break;
            }

            Thread.sleep(100);
        }

        Assert.assertNotNull(taskStatus);
        Assert.assertTrue(taskStatus.hasError());
    }
}
