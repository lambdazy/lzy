package ai.lzy.scheduler.test;

import ai.lzy.model.utils.FreePortFinder;
import ai.lzy.scheduler.allocator.ServantMetaStorage;
import ai.lzy.scheduler.configs.ServantEventProcessorConfig;
import ai.lzy.scheduler.configs.ServiceConfig;
import ai.lzy.scheduler.db.DaoException;
import ai.lzy.scheduler.db.ServantDao;
import ai.lzy.scheduler.db.ServantEventDao;
import ai.lzy.scheduler.db.TaskDao;
import ai.lzy.scheduler.models.ServantState;
import ai.lzy.scheduler.models.TaskDesc;
import ai.lzy.scheduler.servant.ServantsPool;
import ai.lzy.scheduler.servant.impl.EventQueueManager;
import ai.lzy.scheduler.servant.impl.SchedulerImpl;
import ai.lzy.scheduler.servant.impl.ServantsPoolImpl;
import ai.lzy.scheduler.test.EventProcessorTest.AllocationRequest;
import ai.lzy.scheduler.test.mocks.*;
import io.micronaut.context.ApplicationContext;
import org.apache.curator.shaded.com.google.common.net.HostAndPort;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.*;
import java.util.function.Function;

import static ai.lzy.scheduler.test.EventProcessorTest.buildZygote;

public class SchedulerTest {

    public AllocatorMock allocator;
    public ServantEventDao events;
    public ServantDao servantDao;
    public TaskDao tasks;
    public String workflowId;
    public CountDownLatch servantReady;
    public EventQueueManager manager;

    @Rule
    public Timeout globalTimeout = Timeout.seconds(10);

    @Before
    public void setUp() {
        workflowId = "wf";
        var context = ApplicationContext.run();
        tasks = context.getBean(TaskDao.class);
        events = context.getBean(ServantEventDao.class);
        manager = context.getBean(EventQueueManager.class);
        servantDao = context.getBean(ServantDao.class);
        ServantMetaStorage storage = context.getBean(ServantMetaStorage.class);
        allocator = new AllocatorMock(servantDao, storage);
        servantReady = new CountDownLatch(1);
        Configurator.setAllLevels("ai.lzy.scheduler", Level.ALL);
    }

    @Test
    public void testSimple() throws Exception {
        ServiceConfig config = new ServiceConfig(1234, 1, Map.of(), 1, URI.create("http://localhost:1000"),
            URI.create("http://localhost:1000"), "", null, null);
        final ServantEventProcessorConfig processorConfig = new ServantEventProcessorConfig(1, 1, 1, 1, 100, 100);

        ServantsPool pool = new ServantsPoolImpl(config, processorConfig, servantDao, allocator, events, tasks, manager);
        SchedulerImpl scheduler = new SchedulerImpl(servantDao, tasks, pool, config);
        scheduler.start();
        final CompletableFuture<AllocationRequest> allocationRequested = new CompletableFuture<>();
        allocator.onAllocationRequested(((a, b, c) -> allocationRequested.complete(new AllocationRequest(a, b, c))));
        var task1 = scheduler.execute(workflowId, workflowId, new TaskDesc(buildZygote(), Map.of()));
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
        final HostAndPort servantUri = HostAndPort.fromParts("localhost", port);
        awaitState(req.workflowId(), req.servantId(), ServantState.Status.CONNECTING);
        allocator.register(req.workflowId(), req.servantId(), servantUri);

        env.take();
        var servant = servantDao.get(req.workflowId(), req.servantId());
        servant.notifyConfigured(0, "Ok");

        exec.take();
        servant.notifyExecutionCompleted(0, "Ok");

        var task2 = scheduler.execute(workflowId, workflowId, new TaskDesc(buildZygote(), Map.of()));
        var task3 = scheduler.execute(workflowId, workflowId, new TaskDesc(buildZygote(), Map.of()));

        env.take();
        servant.notifyConfigured(0, "OK");

        exec.take();
        servant.notifyExecutionCompleted(0, "Ok");

        env.take();
        servant.notifyConfigured(0, "Ok");

        exec.take();
        servant.notifyExecutionCompleted(0, "Ok");

        awaitState(req.workflowId(), req.servantId(), ServantState.Status.RUNNING);
        servant.notifyCommunicationCompleted();

        awaitState(req.workflowId(), req.servantId(), ServantState.Status.STOPPING);
        servant.notifyStopped(0, "Stopped");

        awaitState(req.workflowId(), req.servantId(), ServantState.Status.DESTROYED);
    }

    @Test
    public void testParallel() throws Exception {
        ServiceConfig config = new ServiceConfig(1234, /*maxServantsPerWorkflow*/2, Map.of(),
                /*maxDefaultServant*/ 2, URI.create("http://localhost:1000"),
                URI.create("http://localhost:1000"), "", null, null);
        final ServantEventProcessorConfig processorConfig = new ServantEventProcessorConfig(1, 1, 1, 1, 100, 100);

        final BlockingQueue<AllocationRequest> requests = new LinkedBlockingQueue<>();
        ServantsPool pool = new ServantsPoolImpl(config, processorConfig, servantDao, allocator, events, tasks, manager);
        SchedulerImpl scheduler = new SchedulerImpl(servantDao, tasks, pool, config);
        scheduler.start();

        allocator.onAllocationRequested(((a, b, c) -> requests.add(new AllocationRequest(a, b, c))));

        var task1 = scheduler.execute(workflowId, workflowId, new TaskDesc(buildZygote(), Map.of()));
        var task2 = scheduler.execute(workflowId, workflowId, new TaskDesc(buildZygote(), Map.of()));

        final var port1 = FreePortFinder.find(1000, 2000);
        final CountDownLatch env1 = new CountDownLatch(1),
            exec1 = new CountDownLatch(1),
            stop1 = new CountDownLatch(1);
        final var mock1 = new AllocatedServantMock.ServantBuilder(port1)
            .setOnEnv(env1::countDown)
            .setOnExec(exec1::countDown)
            .setOnStop(stop1::countDown)
            .build();
        final HostAndPort servantUri1 = HostAndPort.fromParts("localhost", port1);

        final var port2 = FreePortFinder.find(1000, 2000);
        final CountDownLatch env2 = new CountDownLatch(1),
            exec2 = new CountDownLatch(1),
            stop2 = new CountDownLatch(1);
        final var mock2 = new AllocatedServantMock.ServantBuilder(port2)
            .setOnEnv(env2::countDown)
            .setOnExec(exec2::countDown)
            .setOnStop(stop2::countDown)
            .build();
        final HostAndPort servantUri2 = HostAndPort.fromParts("localhost", port2);

        var r1 = requests.take();
        allocator.register(r1.workflowId(), r1.servantId(), servantUri1);

        var r2 = requests.take();
        allocator.register(r2.workflowId(), r2.servantId(), servantUri2);

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

        awaitState(r1.workflowId(), r1.servantId(), ServantState.Status.DESTROYED);
        awaitState(r2.workflowId(), r2.servantId(), ServantState.Status.DESTROYED);
    }

    @Test
    public void testRestart() throws Exception {
        ServiceConfig config = new ServiceConfig(1234, 1, Map.of(), 1, URI.create("http://localhost:1000"),
                URI.create("http://localhost:1000"), "", null, null);
        final ServantEventProcessorConfig processorConfig = new ServantEventProcessorConfig(1, 1, 1, 1, 100, 100);

        ServantsPool pool = new ServantsPoolImpl(config, processorConfig, servantDao, allocator, events, tasks, manager);
        SchedulerImpl scheduler = new SchedulerImpl(servantDao, tasks, pool, config);
        scheduler.start();

        Function<SchedulerImpl, SchedulerImpl> restart = (sh) -> {
            sh.terminate();
            try {
                sh.awaitTermination();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            var poolTmp = new ServantsPoolImpl(config, processorConfig, servantDao, allocator, events, tasks, manager);
            var schedulerTmp = new SchedulerImpl(servantDao, tasks, poolTmp, config);
            schedulerTmp.start();
            return schedulerTmp;
        };

        final CompletableFuture<AllocationRequest> allocationRequested = new CompletableFuture<>();
        allocator.onAllocationRequested(((a, b, c) -> allocationRequested.complete(new AllocationRequest(a, b, c))));
        var task1 = scheduler.execute(workflowId, workflowId, new TaskDesc(buildZygote(), Map.of()));
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
        final HostAndPort servantUri = HostAndPort.fromParts("localhost", port);
        awaitState(req.workflowId(), req.servantId(), ServantState.Status.CONNECTING);
        allocator.register(req.workflowId(), req.servantId(), servantUri);

        env.take();
        var servant = servantDao.get(req.workflowId(), req.servantId());

        scheduler = restart.apply(scheduler);

        servant.notifyConfigured(0, "Ok");

        exec.take();
        servant.notifyExecutionCompleted(0, "Ok");

        scheduler = restart.apply(scheduler);

        var task2 = scheduler.execute(workflowId, workflowId, new TaskDesc(buildZygote(), Map.of()));

        scheduler = restart.apply(scheduler);

        var task3 = scheduler.execute(workflowId, workflowId, new TaskDesc(buildZygote(), Map.of()));

        env.take();
        servant.notifyConfigured(0, "OK");

        scheduler = restart.apply(scheduler);

        exec.take();
        servant.notifyExecutionCompleted(0, "Ok");

        scheduler = restart.apply(scheduler);

        env.take();
        servant.notifyConfigured(0, "Ok");

        scheduler = restart.apply(scheduler);

        exec.take();
        servant.notifyExecutionCompleted(0, "Ok");

        scheduler = restart.apply(scheduler);

        awaitState(req.workflowId(), req.servantId(), ServantState.Status.RUNNING);
        servant.notifyCommunicationCompleted();

        scheduler = restart.apply(scheduler);

        awaitState(req.workflowId(), req.servantId(), ServantState.Status.STOPPING);
        servant.notifyStopped(0, "Stopped");

        scheduler = restart.apply(scheduler);

        awaitState(req.workflowId(), req.servantId(), ServantState.Status.DESTROYED);
    }

    public void awaitState(String workflowId, String servantId,
                           ServantState.Status status) throws InterruptedException, DaoException {
        ServantState.Status s = null;
        var servant = servantDao.get(workflowId, servantId);
        if (servant != null) {
            s = servant.status();
        }
        while (s == null || s != status) {
            Thread.sleep(10);
            servant = servantDao.get(workflowId, servantId);
            if (servant == null) {
                s = null;
            } else {
                s = servant.status();
            }
        }
    }
}
