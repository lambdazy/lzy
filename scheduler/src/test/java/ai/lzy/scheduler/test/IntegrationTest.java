package ai.lzy.scheduler.test;

import ai.lzy.model.grpc.ChannelBuilder;
import ai.lzy.model.utils.FreePortFinder;
import ai.lzy.v1.Operations;
import ai.lzy.v1.SchedulerApi.TaskDesc;
import ai.lzy.v1.SchedulerApi.TaskScheduleRequest;
import ai.lzy.v1.SchedulerGrpc;
import ai.lzy.v1.lzy.SchedulerPrivateApi.RegisterServantRequest;
import ai.lzy.v1.lzy.SchedulerPrivateApi.ServantProgress;
import ai.lzy.v1.lzy.SchedulerPrivateApi.ServantProgress.Configured;
import ai.lzy.v1.lzy.SchedulerPrivateApi.ServantProgress.ExecutionCompleted;
import ai.lzy.v1.lzy.SchedulerPrivateApi.ServantProgress.Finished;
import ai.lzy.v1.lzy.SchedulerPrivateApi.ServantProgressRequest;
import ai.lzy.v1.lzy.SchedulerPrivateGrpc;
import ai.lzy.scheduler.PrivateSchedulerApiImpl;
import ai.lzy.scheduler.SchedulerApi;
import ai.lzy.scheduler.SchedulerApiImpl;
import ai.lzy.scheduler.configs.ServiceConfig;
import ai.lzy.scheduler.test.mocks.AllocatedServantMock;
import ai.lzy.scheduler.test.mocks.AllocatorMock;
import io.grpc.ManagedChannel;
import io.micronaut.context.ApplicationContext;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.*;

public class IntegrationTest {
    private SchedulerApi api;
    private AllocatorMock allocator;
    private SchedulerGrpc.SchedulerBlockingStub stub;
    private SchedulerPrivateGrpc.SchedulerPrivateBlockingStub privateStub;
    ManagedChannel privateChan;
    ManagedChannel chan;

    @Before
    public void setUp() {
        try (var context = ApplicationContext.run()) {
            SchedulerApiImpl impl = context.getBean(SchedulerApiImpl.class);
            PrivateSchedulerApiImpl privateApi = context.getBean(PrivateSchedulerApiImpl.class);
            ServiceConfig config = context.getBean(ServiceConfig.class);
            api = new SchedulerApi(impl, privateApi, config);
            allocator = context.getBean(AllocatorMock.class);
        }
        chan = ChannelBuilder.forAddress("localhost", 2392)
            .usePlaintext()
            .build();
        stub = SchedulerGrpc.newBlockingStub(chan);
        privateChan = ChannelBuilder.forAddress("localhost", 2392)
                .usePlaintext()
                .build();
        privateStub = SchedulerPrivateGrpc.newBlockingStub(privateChan);
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
        allocator.onAllocationRequested(((workflowId, servantId, token) -> {a.complete(servantId);}));
        allocator.onDestroyRequested((workflow, servant) -> latch.countDown());

        var taskStatus = stub.schedule(TaskScheduleRequest.newBuilder()
            .setWorkflowId("wfid")
            .setWorkflowName("wf")
            .setTask(TaskDesc.newBuilder()
                .setZygote(Operations.Zygote.newBuilder()
                    .setName("name")
                    .setProvisioning(Operations.Provisioning.newBuilder().build())
                    .setFuze("")
                    .setDescription("")
                    .setEnv(Operations.EnvSpec.newBuilder().build())
                    .build())
                .build())
            .build());
        var id = a.get();

        final var port = FreePortFinder.find(1000, 2000);
        final BlockingQueue<String> env = new LinkedBlockingQueue<>(),
            exec = new LinkedBlockingQueue<>(),
            stop = new LinkedBlockingQueue<>();
        final var mock = new AllocatedServantMock.ServantBuilder(port)
            .setOnEnv(() -> env.add(""))
            .setOnExec(() -> exec.add(""))
            .setOnStop(() -> stop.add(""))
            .build();

        privateStub.registerServant(RegisterServantRequest.newBuilder()
            .setWorkflowName("wf")
            .setServantId(id)
            .setApiPort(port)
            .build());
        env.take();

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
