package ai.lzy.scheduler.test;

import static ai.lzy.model.db.test.DatabaseTestUtils.preparePostgresConfig;

import ai.lzy.iam.config.IamClientConfiguration;
import ai.lzy.iam.test.BaseTestWithIam;
import ai.lzy.model.utils.FreePortFinder;
import ai.lzy.scheduler.BeanFactory;
import ai.lzy.scheduler.PrivateSchedulerApiImpl;
import ai.lzy.scheduler.SchedulerApi;
import ai.lzy.scheduler.SchedulerApiImpl;
import ai.lzy.scheduler.configs.ServiceConfig;
import ai.lzy.scheduler.db.ServantDao;
import ai.lzy.scheduler.test.mocks.AllocatedServantMock;
import ai.lzy.scheduler.test.mocks.AllocatorMock;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.util.grpc.ClientHeaderInterceptor;
import ai.lzy.util.grpc.GrpcHeaders;
import ai.lzy.v1.common.LME;
import ai.lzy.v1.common.LMO;
import ai.lzy.v1.scheduler.SchedulerApi.TaskScheduleRequest;
import ai.lzy.v1.scheduler.SchedulerGrpc;
import ai.lzy.v1.scheduler.SchedulerPrivateApi.RegisterServantRequest;
import ai.lzy.v1.scheduler.SchedulerPrivateApi.ServantProgress;
import ai.lzy.v1.scheduler.SchedulerPrivateApi.ServantProgress.Configured;
import ai.lzy.v1.scheduler.SchedulerPrivateApi.ServantProgress.ExecutionCompleted;
import ai.lzy.v1.scheduler.SchedulerPrivateApi.ServantProgress.Finished;
import ai.lzy.v1.scheduler.SchedulerPrivateApi.ServantProgressRequest;
import ai.lzy.v1.scheduler.SchedulerPrivateGrpc;
import io.grpc.ManagedChannel;
import io.micronaut.context.ApplicationContext;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class IntegrationTest extends BaseTestWithIam {

    @Rule
    public PreparedDbRule iamDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});
    @Rule
    public PreparedDbRule db = EmbeddedPostgresRules.preparedDatabase(ds -> {});

    private SchedulerApi api;
    private AllocatorMock allocator;
    private SchedulerGrpc.SchedulerBlockingStub stub;
    private SchedulerPrivateGrpc.SchedulerPrivateBlockingStub privateStub;
    ManagedChannel privateChan;
    ManagedChannel chan;

    @Before
    public void setUp() throws IOException {
        super.setUp(preparePostgresConfig("iam", iamDb.getConnectionInfo()));

        final IamClientConfiguration auth;
        try (var context = ApplicationContext.run(preparePostgresConfig("scheduler", db.getConnectionInfo()))) {
            SchedulerApiImpl impl = context.getBean(SchedulerApiImpl.class);
            PrivateSchedulerApiImpl privateApi = context.getBean(PrivateSchedulerApiImpl.class);
            ServiceConfig config = context.getBean(ServiceConfig.class);
            final var dao = context.getBean(ServantDao.class);
            auth = config.getIam();
            api = new SchedulerApi(impl, privateApi, config, new BeanFactory().iamChannel(config), dao);
            allocator = context.getBean(AllocatorMock.class);
        }
        chan = ChannelBuilder.forAddress("localhost", 2392)
            .usePlaintext()
            .build();

        var credentials = auth.createCredentials();
        stub = SchedulerGrpc.newBlockingStub(chan).withInterceptors(
            ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION, credentials::token));

        privateChan = ChannelBuilder.forAddress("localhost", 2392)
            .usePlaintext()
            .build();

        privateStub = SchedulerPrivateGrpc.newBlockingStub(privateChan).withInterceptors(
            ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION, credentials::token));
        Configurator.setAllLevels("ai.lzy.scheduler", Level.ALL);
    }

    @After
    public void tearDown() throws InterruptedException {
        api.close();
        api.awaitTermination();
        privateChan.shutdown();
        privateChan.awaitTermination(100, TimeUnit.SECONDS);
        chan.shutdown();
        chan.awaitTermination(100, TimeUnit.SECONDS);
    }

    @Test
    public void testSimple() throws Exception {
        CompletableFuture<String> a = new CompletableFuture<>();
        var latch = new CountDownLatch(1);
        allocator.onAllocationRequested(((workflowId, servantId, token) -> a.complete(servantId)));
        allocator.onDestroyRequested((workflow, servant) -> latch.countDown());

        //noinspection ResultOfMethodCallIgnored
        stub.schedule(TaskScheduleRequest.newBuilder()
            .setWorkflowId("wfid")
            .setWorkflowName("wf")
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

        new AllocatedServantMock.ServantBuilder(port)
            .setOnEnv(() -> env.add(""))
            .setOnExec(() -> exec.add(""))
            .setOnStop(() -> stop.add(""))
            .build();

        //noinspection ResultOfMethodCallIgnored
        privateStub.registerServant(RegisterServantRequest.newBuilder()
            .setWorkflowName("wf")
            .setServantId(id)
            .setApiPort(port)
            .build());
        env.take();

        //noinspection ResultOfMethodCallIgnored
        privateStub.servantProgress(ServantProgressRequest.newBuilder()
            .setServantId(id)
            .setWorkflowName("wf")
            .setProgress(ServantProgress.newBuilder()
                .setConfigured(Configured.newBuilder()
                    .setOk(Configured.Ok.newBuilder().build())
                    .build())
                .build()
            )
            .build());

        exec.take();

        //noinspection ResultOfMethodCallIgnored
        privateStub.servantProgress(ServantProgressRequest.newBuilder()
            .setServantId(id)
            .setWorkflowName("wf")
            .setProgress(ServantProgress.newBuilder()
                .setExecutionCompleted(ExecutionCompleted.newBuilder()
                    .setDescription("Ok")
                    .setRc(0)
                    .build())
                .build()
            )
            .build());

        stop.take();

        //noinspection ResultOfMethodCallIgnored
        privateStub.servantProgress(ServantProgressRequest.newBuilder()
            .setServantId(id)
            .setWorkflowName("wf")
            .setProgress(ServantProgress.newBuilder()
                .setFinished(Finished.newBuilder().build())
                .build()
            )
            .build());

        latch.await();
    }
}
