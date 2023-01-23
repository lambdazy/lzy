package ai.lzy.scheduler.test;

import ai.lzy.iam.test.BaseTestWithIam;
import ai.lzy.model.utils.FreePortFinder;
import ai.lzy.scheduler.BeanFactory;
import ai.lzy.scheduler.PrivateSchedulerApiImpl;
import ai.lzy.scheduler.SchedulerApi;
import ai.lzy.scheduler.SchedulerApiImpl;
import ai.lzy.scheduler.configs.ServiceConfig;
import ai.lzy.scheduler.db.WorkerDao;
import ai.lzy.scheduler.test.mocks.AllocatedWorkerMock;
import ai.lzy.scheduler.test.mocks.AllocatorMock;
import ai.lzy.util.grpc.GrpcChannels;
import ai.lzy.v1.common.LME;
import ai.lzy.v1.common.LMO;
import ai.lzy.v1.scheduler.SchedulerApi.TaskScheduleRequest;
import ai.lzy.v1.scheduler.SchedulerGrpc;
import ai.lzy.v1.scheduler.SchedulerPrivateApi.RegisterWorkerRequest;
import ai.lzy.v1.scheduler.SchedulerPrivateApi.WorkerProgress;
import ai.lzy.v1.scheduler.SchedulerPrivateApi.WorkerProgress.Configured;
import ai.lzy.v1.scheduler.SchedulerPrivateApi.WorkerProgress.ExecutionCompleted;
import ai.lzy.v1.scheduler.SchedulerPrivateApi.WorkerProgress.Finished;
import ai.lzy.v1.scheduler.SchedulerPrivateApi.WorkerProgressRequest;
import ai.lzy.v1.scheduler.SchedulerPrivateGrpc;
import io.grpc.ManagedChannel;
import io.micronaut.context.ApplicationContext;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

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
    private AllocatorMock allocator;
    private SchedulerGrpc.SchedulerBlockingStub stub;
    private SchedulerPrivateGrpc.SchedulerPrivateBlockingStub privateStub;
    ManagedChannel privateChan;
    ManagedChannel chan;

    @Before
    public void setUp() throws IOException {
        super.setUp(preparePostgresConfig("iam", iamDb.getConnectionInfo()));

        context = ApplicationContext.run(preparePostgresConfig("scheduler", db.getConnectionInfo()));
        var config = context.getBean(ServiceConfig.class);
        config.getIam().setAddress("localhost:" + super.getPort());

        SchedulerApiImpl impl = context.getBean(SchedulerApiImpl.class);
        PrivateSchedulerApiImpl privateApi = context.getBean(PrivateSchedulerApiImpl.class);

        final var dao = context.getBean(WorkerDao.class);
        var auth = config.getIam();
        api = new SchedulerApi(impl, privateApi, config, new BeanFactory().iamChannel(config), dao);
        allocator = context.getBean(AllocatorMock.class);

        var credentials = auth.createRenewableToken();

        chan = newGrpcChannel("localhost", 2392, SchedulerGrpc.SERVICE_NAME);
        stub = newBlockingClient(SchedulerGrpc.newBlockingStub(chan), "Test", () -> credentials.get().token());

        privateChan = newGrpcChannel("localhost", 2392, SchedulerPrivateGrpc.SERVICE_NAME);
        privateStub = newBlockingClient(SchedulerPrivateGrpc.newBlockingStub(privateChan), "Test",
            () -> credentials.get().token());

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
        CompletableFuture<String> a = new CompletableFuture<>();
        var latch = new CountDownLatch(1);
        allocator.onAllocationRequested(((workflowId, workerId, token) -> a.complete(workerId)));
        allocator.onDestroyRequested((workflow, worker) -> latch.countDown());

        //noinspection ResultOfMethodCallIgnored
        stub.schedule(TaskScheduleRequest.newBuilder()
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
        var id = a.get();

        final var port = FreePortFinder.find(1000, 2000);
        final BlockingQueue<String> env = new LinkedBlockingQueue<>();
        final BlockingQueue<String> exec = new LinkedBlockingQueue<>();
        final BlockingQueue<String> stop = new LinkedBlockingQueue<>();

        new AllocatedWorkerMock.WorkerBuilder(port)
            .setOnEnv(() -> env.add(""))
            .setOnExec(() -> exec.add(""))
            .setOnStop(() -> stop.add(""))
            .build();

        //noinspection ResultOfMethodCallIgnored
        privateStub.registerWorker(RegisterWorkerRequest.newBuilder()
            .setWorkflowName("wf")
            .setWorkerId(id)
            .setAddress("localhost:" + port)
            .build());
        env.take();

        //noinspection ResultOfMethodCallIgnored
        privateStub.workerProgress(WorkerProgressRequest.newBuilder()
            .setWorkerId(id)
            .setWorkflowName("wf")
            .setProgress(WorkerProgress.newBuilder()
                .setConfigured(Configured.newBuilder()
                    .setOk(Configured.Ok.newBuilder().build())
                    .build())
                .build()
            )
            .build());

        exec.take();

        //noinspection ResultOfMethodCallIgnored
        privateStub.workerProgress(WorkerProgressRequest.newBuilder()
            .setWorkerId(id)
            .setWorkflowName("wf")
            .setProgress(WorkerProgress.newBuilder()
                .setExecutionCompleted(ExecutionCompleted.newBuilder()
                    .setDescription("Ok")
                    .setRc(0)
                    .build())
                .build()
            )
            .build());

        stop.take();

        //noinspection ResultOfMethodCallIgnored
        privateStub.workerProgress(WorkerProgressRequest.newBuilder()
            .setWorkerId(id)
            .setWorkflowName("wf")
            .setProgress(WorkerProgress.newBuilder()
                .setFinished(Finished.newBuilder().build())
                .build()
            )
            .build());

        latch.await();
    }
}
