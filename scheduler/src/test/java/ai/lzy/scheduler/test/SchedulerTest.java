package ai.lzy.scheduler.test;

import ai.lzy.model.TaskDesc;
import ai.lzy.model.db.DaoException;
import ai.lzy.model.db.test.DatabaseTestUtils;
import ai.lzy.model.utils.FreePortFinder;
import ai.lzy.scheduler.configs.ProcessorConfigBuilder;
import ai.lzy.scheduler.configs.ServiceConfig;
import ai.lzy.scheduler.db.ServantDao;
import ai.lzy.scheduler.db.ServantEventDao;
import ai.lzy.scheduler.db.TaskDao;
import ai.lzy.scheduler.models.ServantState;
import ai.lzy.scheduler.servant.Servant;
import ai.lzy.scheduler.servant.ServantsPool;
import ai.lzy.scheduler.servant.impl.EventQueueManager;
import ai.lzy.scheduler.servant.impl.SchedulerImpl;
import ai.lzy.scheduler.servant.impl.ServantsPoolImpl;
import ai.lzy.scheduler.test.EventProcessorTest.AllocationRequest;
import ai.lzy.scheduler.test.mocks.AllocatedServantMock;
import ai.lzy.scheduler.test.mocks.AllocatorMock;
import io.micronaut.context.ApplicationContext;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import org.apache.curator.shaded.com.google.common.net.HostAndPort;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;

import static ai.lzy.scheduler.test.EventProcessorTest.buildOp;

public class SchedulerTest {

    @Rule
    public PreparedDbRule db = EmbeddedPostgresRules.preparedDatabase(ds -> {});

    public ApplicationContext context;
    public ServantEventDao events;
    public ServantDao servantDao;
    public TaskDao tasks;
    public EventQueueManager manager;

    public AllocatorMock allocator;
    public String workflowId;
    public String workflowName;

    @Rule
    public Timeout globalTimeout = Timeout.seconds(30);

    @Before
    public void setUp() {
        context = ApplicationContext.run(DatabaseTestUtils.preparePostgresConfig("scheduler", db.getConnectionInfo()));

        events = context.getBean(ServantEventDao.class);
        servantDao = context.getBean(ServantDao.class);
        tasks = context.getBean(TaskDao.class);
        manager = context.getBean(EventQueueManager.class);

        workflowId = "wfid" + UUID.randomUUID();
        workflowName = "wf" + UUID.randomUUID();
        allocator = new AllocatorMock();
    }

    @After
    public void tearDown() throws DaoException {
        for (Servant servant : servantDao.get(workflowId)) {
            servantDao.invalidate(servant, "destroy");
            events.removeAll(servant.id());
        }
        for (var task : tasks.list(workflowId)) {
            task.notifyExecutionCompleted(1, "End of test");
        }
        context.close();
    }

    @Test
    public void testSimple() throws Exception {
        ServiceConfig config = buildConfig(1);
        var processorConfig = new ProcessorConfigBuilder()
            .setIdleTimeout(100)
            .build();

        ServantsPool pool = new ServantsPoolImpl(config, processorConfig, servantDao, allocator, events, tasks,
            manager);
        SchedulerImpl scheduler = new SchedulerImpl(servantDao, tasks, pool, config);
        scheduler.start();
        final CompletableFuture<AllocationRequest> allocationRequested = new CompletableFuture<>();
        allocator.onAllocationRequested(((a, b, c) -> allocationRequested.complete(new AllocationRequest(a, b, c))));

        scheduler.execute(workflowId, workflowName, new TaskDesc(buildOp(), Map.of())); //task 1
        var req = allocationRequested.get();

        final var port = FreePortFinder.find(1000, 2000);
        final BlockingQueue<String> env = new LinkedBlockingQueue<>();
        final BlockingQueue<String> exec = new LinkedBlockingQueue<>();
        //noinspection MismatchedQueryAndUpdateOfCollection
        final BlockingQueue<String> stop = new LinkedBlockingQueue<>();
        new AllocatedServantMock.ServantBuilder(port)
            .setOnEnv(() -> env.add(""))
            .setOnExec(() -> exec.add(""))
            .setOnStop(() -> stop.add(""))
            .build();
        final HostAndPort servantUri = HostAndPort.fromParts("localhost", port);
        awaitState(req.workflowId(), req.servantId(), ServantState.Status.CONNECTING);
        var servant = servantDao.get(req.workflowId(), req.servantId());
        Objects.requireNonNull(servant).notifyConnected(servantUri);

        env.take();
        servant.notifyConfigured(0, "Ok");

        exec.take();
        servant.notifyExecutionCompleted(0, "Ok");

        scheduler.execute(workflowId, workflowName, new TaskDesc(buildOp(), Map.of())); //task 2
        scheduler.execute(workflowId, workflowName, new TaskDesc(buildOp(), Map.of())); //task 3

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
        ServiceConfig config = buildConfig(2);
        var processorConfig = new ProcessorConfigBuilder()
            .setIdleTimeout(100)
            .build();

        final BlockingQueue<AllocationRequest> requests = new LinkedBlockingQueue<>();
        ServantsPool pool = new ServantsPoolImpl(config, processorConfig, servantDao, allocator, events, tasks,
            manager);
        SchedulerImpl scheduler = new SchedulerImpl(servantDao, tasks, pool, config);
        scheduler.start();

        allocator.onAllocationRequested(((a, b, c) -> requests.add(new AllocationRequest(a, b, c))));

        scheduler.execute(workflowId, workflowName, new TaskDesc(buildOp(), Map.of())); //task 1
        scheduler.execute(workflowId, workflowName, new TaskDesc(buildOp(), Map.of())); //task 2

        final var port1 = FreePortFinder.find(1000, 2000);
        final CountDownLatch env1 = new CountDownLatch(1);
        final CountDownLatch exec1 = new CountDownLatch(1);
        final CountDownLatch stop1 = new CountDownLatch(1);

        new AllocatedServantMock.ServantBuilder(port1)
            .setOnEnv(env1::countDown)
            .setOnExec(exec1::countDown)
            .setOnStop(stop1::countDown)
            .build();
        final HostAndPort servantUri1 = HostAndPort.fromParts("localhost", port1);

        final var port2 = FreePortFinder.find(1000, 2000);
        final CountDownLatch env2 = new CountDownLatch(1);
        final CountDownLatch exec2 = new CountDownLatch(1);
        final CountDownLatch stop2 = new CountDownLatch(1);

        new AllocatedServantMock.ServantBuilder(port2)
            .setOnEnv(env2::countDown)
            .setOnExec(exec2::countDown)
            .setOnStop(stop2::countDown)
            .build();
        final HostAndPort servantUri2 = HostAndPort.fromParts("localhost", port2);

        var r1 = requests.take();
        var servant1 = servantDao.get(r1.workflowId(), r1.servantId());
        Objects.requireNonNull(servant1).notifyConnected(servantUri1);

        var r2 = requests.take();
        var servant2 = servantDao.get(r2.workflowId(), r2.servantId());
        Objects.requireNonNull(servant2).notifyConnected(servantUri2);

        env1.await();
        env2.await();

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

    @NotNull
    private ServiceConfig buildConfig(int maxServants) {
        ServiceConfig config = new ServiceConfig();
        config.setMaxServantsPerWorkflow(maxServants);
        config.setDefaultProvisioningLimit(maxServants);
        config.setProvisioningLimits(Map.of());
        return config;
    }

    @Test
    public void testRestart() throws Exception {
        ServiceConfig config = buildConfig(1);
        var processorConfig = new ProcessorConfigBuilder()
            .setIdleTimeout(100)
            .build();

        ServantsPool pool = new ServantsPoolImpl(config, processorConfig, servantDao, allocator, events, tasks,
            manager);
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
        scheduler.execute(workflowId, workflowName, new TaskDesc(buildOp(), Map.of())); //task 1
        var req = allocationRequested.get();

        final var port = FreePortFinder.find(1000, 2000);
        final BlockingQueue<String> env = new LinkedBlockingQueue<>();
        final BlockingQueue<String> exec = new LinkedBlockingQueue<>();
        //noinspection MismatchedQueryAndUpdateOfCollection
        final BlockingQueue<String> stop = new LinkedBlockingQueue<>();

        new AllocatedServantMock.ServantBuilder(port)
            .setOnEnv(() -> env.add(""))
            .setOnExec(() -> exec.add(""))
            .setOnStop(() -> stop.add(""))
            .build();
        final HostAndPort servantUri = HostAndPort.fromParts("localhost", port);
        awaitState(req.workflowId(), req.servantId(), ServantState.Status.CONNECTING);
        var servant = servantDao.get(req.workflowId(), req.servantId());
        Objects.requireNonNull(servant).notifyConnected(servantUri);

        env.take();

        scheduler = restart.apply(scheduler);

        servant.notifyConfigured(0, "Ok");

        exec.take();
        servant.notifyExecutionCompleted(0, "Ok");

        scheduler = restart.apply(scheduler);

        scheduler.execute(workflowId, workflowName, new TaskDesc(buildOp(), Map.of())); //task 2

        scheduler = restart.apply(scheduler);

        scheduler.execute(workflowId, workflowName, new TaskDesc(buildOp(), Map.of())); //task 3

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

        //noinspection UnusedAssignment
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
