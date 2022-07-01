package ai.lzy.scheduler.test;

import ai.lzy.model.utils.FreePortFinder;
import ai.lzy.scheduler.configs.ServantEventProcessorConfig;
import ai.lzy.scheduler.configs.ServiceConfig;
import ai.lzy.scheduler.models.ServantState;
import ai.lzy.scheduler.models.TaskDesc;
import ai.lzy.scheduler.servant.ServantsPool;
import ai.lzy.scheduler.servant.impl.EventQueueManager;
import ai.lzy.scheduler.servant.impl.SchedulerImpl;
import ai.lzy.scheduler.servant.impl.ServantsPoolImpl;
import ai.lzy.scheduler.test.EventProcessorTest.AllocationRequest;
import ai.lzy.scheduler.test.mocks.*;
import io.grpc.StatusException;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.*;

import static ai.lzy.scheduler.test.EventProcessorTest.buildZygote;

public class SchedulerTest {

    public AllocatorMock allocator;
    public EventDaoMock events;
    public ServantDaoMock servantDao;
    public TaskDaoMock tasks;
    public String workflowId;
    public CountDownLatch servantReady;
    public EventQueueManager manager;

    @Rule
    public Timeout globalTimeout = Timeout.seconds(10);

    @Before
    public void setUp() {
        workflowId = "wf";
        tasks = new TaskDaoMock();
        events = new EventDaoMock();
        manager = new EventQueueManager(events);
        servantDao = new ServantDaoMock(manager);
        allocator = new AllocatorMock(servantDao);
        servantReady = new CountDownLatch(1);
        Configurator.setAllLevels("ai.lzy.scheduler", Level.ALL);
    }

    @Test
    public void testSimple() throws StatusException, ExecutionException, InterruptedException, IOException {
        ServiceConfig config = new ServiceConfig(1, Map.of(), 1, URI.create("http://localhost:1000"),
            URI.create("http://localhost:1000"), "", null, null);
        final ServantEventProcessorConfig processorConfig = new ServantEventProcessorConfig(2, 2, 2, 2, 100, 100);

        ServantsPool pool = new ServantsPoolImpl(config, processorConfig, servantDao, allocator, events, tasks, manager);
        SchedulerImpl scheduler = new SchedulerImpl(servantDao, tasks, pool, config);
        scheduler.start();
        final CompletableFuture<AllocationRequest> allocationRequested = new CompletableFuture<>();
        allocator.onAllocationRequested(((a, b, c) -> allocationRequested.complete(new AllocationRequest(a, b, c))));
        var task1 = scheduler.execute(workflowId, new TaskDesc(buildZygote(), Map.of()));
        var req = allocationRequested.get();

        final var port = FreePortFinder.find(1000, 2000);
        final BlockingQueue<String> env = new LinkedBlockingQueue<>(),
                exec = new LinkedBlockingQueue<>(),
                stop = new LinkedBlockingQueue<>();
        final var mock = new AllocatedServantMock.ServantBuilder(port)
                .setOnEnv(() -> env.add(""))
                .setOnExec(() -> exec.add(""))
                .setOnStop(() -> stop.add(""))
                .build();
        final URI servantUri = URI.create("http://localhost:" + port);
        servantDao.awaitState(req.workflowId(), req.servantId(), ServantState.Status.CONNECTING);
        allocator.register(req.workflowId(), req.servantId(), servantUri, req.token());

        env.take();
        var servant = servantDao.get(req.workflowId(), req.servantId());
        servant.notifyConfigured(0, "Ok");

        exec.take();
        servant.notifyExecutionCompleted(0, "Ok");

        var task2 = scheduler.execute(workflowId, new TaskDesc(buildZygote(), Map.of()));
        var task3 = scheduler.execute(workflowId, new TaskDesc(buildZygote(), Map.of()));

        env.take();
        servant.notifyConfigured(0, "OK");

        exec.take();
        servant.notifyExecutionCompleted(0, "Ok");

        env.take();
        servant.notifyConfigured(0, "Ok");

        exec.take();
        servant.notifyExecutionCompleted(0, "Ok");

        servantDao.awaitState(req.workflowId(), req.servantId(), ServantState.Status.RUNNING);
        servant.notifyCommunicationCompleted();

        servantDao.awaitState(req.workflowId(), req.servantId(), ServantState.Status.STOPPING);
        servant.notifyStopped(0, "Stopped");

        servantDao.awaitState(req.workflowId(), req.servantId(), ServantState.Status.DESTROYED);
    }

    @Test
    public void testParallel() throws InterruptedException, StatusException, IOException {
        ServiceConfig config = new ServiceConfig(/*maxServantsPerWorkflow*/2, Map.of(),
                /*maxDefaultServant*/ 2, URI.create("http://localhost:1000"),
                URI.create("http://localhost:1000"), "", null, null);
        final ServantEventProcessorConfig processorConfig = new ServantEventProcessorConfig(2, 2, 2, 2, 100, 100);

        final BlockingQueue<AllocationRequest> requests = new LinkedBlockingQueue<>();
        ServantsPool pool = new ServantsPoolImpl(config, processorConfig, servantDao, allocator, events, tasks, manager);
        SchedulerImpl scheduler = new SchedulerImpl(servantDao, tasks, pool, config);
        scheduler.start();

        allocator.onAllocationRequested(((a, b, c) -> requests.add(new AllocationRequest(a, b, c))));

        var task1 = scheduler.execute(workflowId, new TaskDesc(buildZygote(), Map.of()));
        var task2 = scheduler.execute(workflowId, new TaskDesc(buildZygote(), Map.of()));

        final var port1 = FreePortFinder.find(1000, 2000);
        final CountDownLatch env1 = new CountDownLatch(1),
            exec1 = new CountDownLatch(1),
            stop1 = new CountDownLatch(1);
        final var mock1 = new AllocatedServantMock.ServantBuilder(port1)
            .setOnEnv(env1::countDown)
            .setOnExec(exec1::countDown)
            .setOnStop(stop1::countDown)
            .build();
        final URI servantUri1 = URI.create("http://localhost:" + port1);

        final var port2 = FreePortFinder.find(1000, 2000);
        final CountDownLatch env2 = new CountDownLatch(1),
            exec2 = new CountDownLatch(1),
            stop2 = new CountDownLatch(1);
        final var mock2 = new AllocatedServantMock.ServantBuilder(port2)
            .setOnEnv(env2::countDown)
            .setOnExec(exec2::countDown)
            .setOnStop(stop2::countDown)
            .build();
        final URI servantUri2 = URI.create("http://localhost:" + port2);

        var r1 = requests.take();
        allocator.register(r1.workflowId(), r1.servantId(), servantUri1, r1.token());

        var r2 = requests.take();
        allocator.register(r2.workflowId(), r2.servantId(), servantUri2, r2.token());

        env1.await();
        env2.await();

        var servant1 = servantDao.get(r1.workflowId(), r1.servantId());
        var servant2 = servantDao.get(r2.workflowId(), r2.servantId());

        servant1.notifyConfigured(0, "Ok");
        servant2.notifyConfigured(0, "Ok");

        exec1.await();
        exec2.await();

        servant1.notifyExecutionCompleted(0, "Ok");
        servant2.notifyExecutionCompleted(0, "Ok");

        stop1.await();
        stop2.await();

        servant1.notifyStopped(0, "Ok");
        servant2.notifyStopped(0, "Ok");

        servantDao.awaitState(r1.workflowId(), r1.servantId(), ServantState.Status.DESTROYED);
        servantDao.awaitState(r2.workflowId(), r2.servantId(), ServantState.Status.DESTROYED);
    }
}
