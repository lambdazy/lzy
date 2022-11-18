package ai.lzy.scheduler.test;

import ai.lzy.model.ReturnCodes;
import ai.lzy.model.TaskDesc;
import ai.lzy.model.db.exceptions.DaoException;
import ai.lzy.model.db.test.DatabaseTestUtils;
import ai.lzy.model.utils.FreePortFinder;
import ai.lzy.scheduler.configs.ProcessorConfigBuilder;
import ai.lzy.scheduler.configs.ServiceConfig;
import ai.lzy.scheduler.db.ServantDao;
import ai.lzy.scheduler.db.ServantEventDao;
import ai.lzy.scheduler.db.TaskDao;
import ai.lzy.scheduler.models.ServantState;
import ai.lzy.scheduler.models.TaskState;
import ai.lzy.scheduler.servant.Servant;
import ai.lzy.scheduler.servant.ServantsPool;
import ai.lzy.scheduler.servant.impl.EventQueueManager;
import ai.lzy.scheduler.servant.impl.SchedulerImpl;
import ai.lzy.scheduler.servant.impl.ServantsPoolImpl;
import ai.lzy.scheduler.test.EventProcessorTest.AllocationRequest;
import ai.lzy.scheduler.test.mocks.AllocatedServantMock;
import ai.lzy.scheduler.test.mocks.AllocatorMock;
import com.google.common.net.HostAndPort;
import io.grpc.Status;
import io.micronaut.context.ApplicationContext;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.IOException;
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
    public String userId;

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
        userId = "uid" + UUID.randomUUID();
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

        scheduler.execute(workflowId, workflowName, userId, new TaskDesc(buildOp(), Map.of())); //task 1
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

        scheduler.execute(workflowId, workflowName, userId, new TaskDesc(buildOp(), Map.of())); //task 2
        scheduler.execute(workflowId, workflowName, userId, new TaskDesc(buildOp(), Map.of())); //task 3

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
        SchedulerImpl scheduler = createScheduler(2);

        final BlockingQueue<AllocationRequest> requests = new LinkedBlockingQueue<>();
        allocator.onAllocationRequested(((a, b, c) -> requests.add(new AllocationRequest(a, b, c))));

        scheduler.execute(workflowId, workflowName, userId, new TaskDesc(buildOp(), Map.of())); //task 1
        scheduler.execute(workflowId, workflowName, userId, new TaskDesc(buildOp(), Map.of())); //task 2

        final var state1 = new ServantTestState();
        final var state2 = new ServantTestState();

        var r1 = requests.take();
        var servant1 = servantDao.get(r1.workflowId(), r1.servantId());
        Objects.requireNonNull(servant1).notifyConnected(state1.servantUri);

        var r2 = requests.take();
        var servant2 = servantDao.get(r2.workflowId(), r2.servantId());
        Objects.requireNonNull(servant2).notifyConnected(state2.servantUri);

        state1.env.await();
        state2.env.await();

        servant1.notifyConfigured(0, "Ok");
        servant2.notifyConfigured(0, "Ok");

        state1.exec.await();
        state2.exec.await();

        servant1.notifyExecutionCompleted(0, "Ok");
        servant2.notifyExecutionCompleted(0, "Ok");

        state1.stop.await();
        state2.stop.await();

        servant1.notifyStopped(0, "Ok");
        servant2.notifyStopped(0, "Ok");

        awaitState(r1.workflowId(), r1.servantId(), ServantState.Status.DESTROYED);
        awaitState(r2.workflowId(), r2.servantId(), ServantState.Status.DESTROYED);
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
        scheduler.execute(workflowId, workflowName, userId, new TaskDesc(buildOp(), Map.of())); //task 1
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

        scheduler.execute(workflowId, workflowName, userId, new TaskDesc(buildOp(), Map.of())); //task 2

        scheduler = restart.apply(scheduler);

        scheduler.execute(workflowId, workflowName, userId, new TaskDesc(buildOp(), Map.of())); //task 3

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

    @Test
    public void testFailOnEnv() throws Exception {
        SchedulerImpl scheduler = createScheduler(1);

        final CompletableFuture<AllocationRequest> allocationRequested = new CompletableFuture<>();
        allocator.onAllocationRequested(((a, b, c) -> allocationRequested.complete(new AllocationRequest(a, b, c))));

        scheduler.execute(workflowId, workflowName, userId, new TaskDesc(buildOp(), Map.of()));
        /// WHEN
        final var state1 = new ServantTestState(true, false, false);

        var req = allocationRequested.get();
        awaitState(req.workflowId(), req.servantId(), ServantState.Status.CONNECTING);
        var servant = servantDao.get(req.workflowId(), req.servantId());
        Objects.requireNonNull(servant).notifyConnected(state1.servantUri);

        state1.env.await();

        /// THEN
        awaitState(req.workflowId(), req.servantId(), ServantState.Status.DESTROYED);

        for (var task : tasks.list(workflowId)) {
            Assert.assertEquals(TaskState.Status.ERROR, task.status());
            Assert.assertEquals("Internal error", task.errorDescription());
        }
    }

    @Test
    public void testFailOnExec() throws Exception {
        SchedulerImpl scheduler = createScheduler(1);

        final CompletableFuture<AllocationRequest> allocationRequested = new CompletableFuture<>();
        allocator.onAllocationRequested(((a, b, c) -> allocationRequested.complete(new AllocationRequest(a, b, c))));

        scheduler.execute(workflowId, workflowName, userId, new TaskDesc(buildOp(), Map.of()));
        /// WHEN
        final var state1 = new ServantTestState(false, true, false);

        var req = allocationRequested.get();
        awaitState(req.workflowId(), req.servantId(), ServantState.Status.CONNECTING);
        var servant = servantDao.get(req.workflowId(), req.servantId());
        Objects.requireNonNull(servant).notifyConnected(state1.servantUri);

        state1.env.await();
        servant.notifyConfigured(0, "Ok");
        state1.exec.await();

        /// THEN
        awaitState(req.workflowId(), req.servantId(), ServantState.Status.DESTROYED);

        for (var task : tasks.list(workflowId)) {
            Assert.assertEquals(TaskState.Status.ERROR, task.status());
            Assert.assertEquals("Internal error", task.errorDescription());
        }
    }

    @Test
    public void testFailOnStop() throws Exception {
        SchedulerImpl scheduler = createScheduler(1);

        final CompletableFuture<AllocationRequest> allocationRequested = new CompletableFuture<>();
        allocator.onAllocationRequested(((a, b, c) -> allocationRequested.complete(new AllocationRequest(a, b, c))));

        scheduler.execute(workflowId, workflowName, userId, new TaskDesc(buildOp(), Map.of()));
        /// WHEN
        final var state1 = new ServantTestState(false, false, true);

        var req = allocationRequested.get();
        awaitState(req.workflowId(), req.servantId(), ServantState.Status.CONNECTING);
        var servant = servantDao.get(req.workflowId(), req.servantId());
        Objects.requireNonNull(servant).notifyConnected(state1.servantUri);

        state1.env.await();
        servant.notifyConfigured(0, "Ok");
        state1.exec.await();
        servant.notifyExecutionCompleted(0, "Ok");
        state1.stop.await();

        /// THEN
        awaitState(req.workflowId(), req.servantId(), ServantState.Status.DESTROYED);

        for (var task : tasks.list(workflowId)) {
            Assert.assertEquals(TaskState.Status.SUCCESS, task.status());
        }
    }

    @Test
    public void testConfigurationTimeout() throws Exception {
        SchedulerImpl scheduler = createScheduler(1);

        final CompletableFuture<AllocationRequest> allocationRequested = new CompletableFuture<>();
        allocator.onAllocationRequested(((a, b, c) -> allocationRequested.complete(new AllocationRequest(a, b, c))));

        scheduler.execute(workflowId, workflowName, userId, new TaskDesc(buildOp(), Map.of()));
        final var state1 = new ServantTestState();

        var req = allocationRequested.get();
        awaitState(req.workflowId(), req.servantId(), ServantState.Status.CONNECTING);
        var servant = servantDao.get(req.workflowId(), req.servantId());
        Objects.requireNonNull(servant).notifyConnected(state1.servantUri);

        state1.env.await();
        /// WHEN - not notified

        /// THEN
        awaitState(req.workflowId(), req.servantId(), ServantState.Status.DESTROYED);

        for (var task : tasks.list(workflowId)) {
            Assert.assertEquals(TaskState.Status.ERROR, task.status());
            Assert.assertEquals("Servant stopping: <Environment is installing too long.>", task.errorDescription());
        }
    }
    @Test
    public void testExecutionTimeout() throws Exception {
        SchedulerImpl scheduler = createScheduler(1);

        final CompletableFuture<AllocationRequest> allocationRequested = new CompletableFuture<>();
        allocator.onAllocationRequested(((a, b, c) -> allocationRequested.complete(new AllocationRequest(a, b, c))));

        scheduler.execute(workflowId, workflowName, userId, new TaskDesc(buildOp(), Map.of()));
        final var state1 = new ServantTestState();

        var req = allocationRequested.get();
        awaitState(req.workflowId(), req.servantId(), ServantState.Status.CONNECTING);
        var servant = servantDao.get(req.workflowId(), req.servantId());
        Objects.requireNonNull(servant).notifyConnected(state1.servantUri);

        state1.env.await();
        servant.notifyConfigured(0, "Ok");
        state1.exec.await();
        servant.executingHeartbeat();
        /// WHEN - not notified

        /// THEN
        awaitState(req.workflowId(), req.servantId(), ServantState.Status.DESTROYED);

        for (var task : tasks.list(workflowId)) {
            Assert.assertEquals(TaskState.Status.ERROR, task.status());
            Assert.assertEquals("Servant is dead", task.errorDescription());
        }
    }

    @Test
    public void testStopTimeout() throws Exception {
        SchedulerImpl scheduler = createScheduler(1);

        final CompletableFuture<AllocationRequest> allocationRequested = new CompletableFuture<>();
        allocator.onAllocationRequested(((a, b, c) -> allocationRequested.complete(new AllocationRequest(a, b, c))));

        scheduler.execute(workflowId, workflowName, userId, new TaskDesc(buildOp(), Map.of()));
        final var state1 = new ServantTestState();

        var req = allocationRequested.get();
        awaitState(req.workflowId(), req.servantId(), ServantState.Status.CONNECTING);
        var servant = servantDao.get(req.workflowId(), req.servantId());
        Objects.requireNonNull(servant).notifyConnected(state1.servantUri);

        state1.env.await();
        servant.notifyConfigured(0, "Ok");
        state1.exec.await();
        servant.notifyExecutionCompleted(0, "Ok");
        state1.stop.await();
        /// WHEN - not notified

        /// THEN
        awaitState(req.workflowId(), req.servantId(), ServantState.Status.DESTROYED);

        for (var task : tasks.list(workflowId)) {
            Assert.assertEquals(TaskState.Status.SUCCESS, task.status());
        }
    }

    @Test
    public void testFailOnEnvNotifying() throws Exception {
        SchedulerImpl scheduler = createScheduler(1);

        final CompletableFuture<AllocationRequest> allocationRequested = new CompletableFuture<>();
        allocator.onAllocationRequested(((a, b, c) -> allocationRequested.complete(new AllocationRequest(a, b, c))));

        scheduler.execute(workflowId, workflowName, userId, new TaskDesc(buildOp(), Map.of()));
        final var state1 = new ServantTestState();

        var req = allocationRequested.get();
        awaitState(req.workflowId(), req.servantId(), ServantState.Status.CONNECTING);
        var servant = servantDao.get(req.workflowId(), req.servantId());
        Objects.requireNonNull(servant).notifyConnected(state1.servantUri);

        state1.env.await();

        /// WHEN
        servant.notifyConfigured(ReturnCodes.ENVIRONMENT_INSTALLATION_ERROR.getRc(), "test");

        /// THEN
        awaitState(req.workflowId(), req.servantId(), ServantState.Status.STOPPING);
        servant.notifyStopped(0, "Stopped");
        awaitState(req.workflowId(), req.servantId(), ServantState.Status.DESTROYED);

        for (var task : tasks.list(workflowId)) {
            Assert.assertEquals(TaskState.Status.ERROR, task.status());
            Assert.assertEquals("Error while configuring servant: test", task.errorDescription());
        }
    }

    @Test
    public void testFailOnExecNotifying() throws Exception {
        SchedulerImpl scheduler = createScheduler(1);

        final CompletableFuture<AllocationRequest> allocationRequested = new CompletableFuture<>();
        allocator.onAllocationRequested(((a, b, c) -> allocationRequested.complete(new AllocationRequest(a, b, c))));

        scheduler.execute(workflowId, workflowName, userId, new TaskDesc(buildOp(), Map.of()));
        final var state1 = new ServantTestState();

        var req = allocationRequested.get();
        awaitState(req.workflowId(), req.servantId(), ServantState.Status.CONNECTING);
        var servant = servantDao.get(req.workflowId(), req.servantId());
        Objects.requireNonNull(servant).notifyConnected(state1.servantUri);

        state1.env.await();
        servant.notifyConfigured(0, "Ok");
        state1.exec.await();

        /// WHEN
        servant.notifyExecutionCompleted(ReturnCodes.ENVIRONMENT_INSTALLATION_ERROR.getRc(), "test");

        /// THEN
        awaitState(req.workflowId(), req.servantId(), ServantState.Status.STOPPING);
        servant.notifyStopped(0, "Stopped");
        awaitState(req.workflowId(), req.servantId(), ServantState.Status.DESTROYED);

        for (var task : tasks.list(workflowId)) {
            Assert.assertEquals(TaskState.Status.ERROR, task.status());
            Assert.assertEquals("test", task.errorDescription());
        }
    }

    @Test
    public void testFailOnStopNotifying() throws Exception {
        SchedulerImpl scheduler = createScheduler(1);

        final CompletableFuture<AllocationRequest> allocationRequested = new CompletableFuture<>();
        allocator.onAllocationRequested(((a, b, c) -> allocationRequested.complete(new AllocationRequest(a, b, c))));

        scheduler.execute(workflowId, workflowName, userId, new TaskDesc(buildOp(), Map.of()));
        final var state1 = new ServantTestState();

        var req = allocationRequested.get();
        awaitState(req.workflowId(), req.servantId(), ServantState.Status.CONNECTING);
        var servant = servantDao.get(req.workflowId(), req.servantId());
        Objects.requireNonNull(servant).notifyConnected(state1.servantUri);

        state1.env.await();
        servant.notifyConfigured(0, "Ok");
        state1.exec.await();
        servant.notifyExecutionCompleted(0, "Ok");
        state1.stop.await();

        /// WHEN
        servant.notifyStopped(ReturnCodes.ENVIRONMENT_INSTALLATION_ERROR.getRc(), "test");

        /// THEN
        awaitState(req.workflowId(), req.servantId(), ServantState.Status.DESTROYED);

        for (var task : tasks.list(workflowId)) {
            Assert.assertEquals(TaskState.Status.SUCCESS, task.status());
        }
    }

    @Test
    public void testFailingServantDontAffectOther() throws Exception {
        SchedulerImpl scheduler = createScheduler(2);

        final BlockingQueue<AllocationRequest> requests = new LinkedBlockingQueue<>();
        allocator.onAllocationRequested(((a, b, c) -> requests.add(new AllocationRequest(a, b, c))));

        var workflowId2 = "wfid" + UUID.randomUUID();
        var workflowName2 = "wf" + UUID.randomUUID();
        scheduler.execute(workflowId, workflowName, userId, new TaskDesc(buildOp(), Map.of())); //task 1
        scheduler.execute(workflowId2, workflowName2, userId, new TaskDesc(buildOp(), Map.of())); //task 2

        /// WHEN
        final var state1 = new ServantTestState(true, false, false);
        final var state2 = new ServantTestState();

        var r1 = requests.take();
        var servant1 = servantDao.get(r1.workflowId(), r1.servantId());
        Objects.requireNonNull(servant1).notifyConnected(state1.servantUri);

        var r2 = requests.take();
        var servant2 = servantDao.get(r2.workflowId(), r2.servantId());
        Objects.requireNonNull(servant2).notifyConnected(state2.servantUri);

        state1.env.await();
        state2.env.await();
        servant2.notifyConfigured(0, "Ok");

        state2.exec.await();
        servant2.notifyExecutionCompleted(0, "Ok");

        awaitState(r2.workflowId(), r2.servantId(), ServantState.Status.RUNNING);
        servant2.notifyCommunicationCompleted();

        awaitState(r2.workflowId(), r2.servantId(), ServantState.Status.STOPPING);
        servant2.notifyStopped(0, "Stopped");

        awaitState(r2.workflowId(), r2.servantId(), ServantState.Status.DESTROYED);

        /// THEN
        awaitState(r1.workflowId(), r1.servantId(), ServantState.Status.DESTROYED);

        for (var task : tasks.list(workflowId)) {
            Assert.assertEquals(TaskState.Status.ERROR, task.status());
            Assert.assertEquals("Internal error", task.errorDescription());
        }

        for (var task : tasks.list(workflowId2)) {
            Assert.assertEquals(TaskState.Status.SUCCESS, task.status());
        }
    }

    private static class ServantTestState {
        final CountDownLatch env = new CountDownLatch(1);
        final CountDownLatch exec = new CountDownLatch(1);
        final CountDownLatch stop = new CountDownLatch(1);
        final HostAndPort servantUri;

        public ServantTestState() throws IOException {
            this(false, false, false);
        }

        public ServantTestState(boolean failEnv, boolean failExec, boolean failStop) throws IOException {
            final var port = FreePortFinder.find(1000, 2000);
            new AllocatedServantMock.ServantBuilder(port)
                .setOnEnv(() -> {
                    env.countDown();
                    if (failEnv) {
                        throw Status.INTERNAL.asRuntimeException();
                    }
                })
                .setOnExec(() -> {
                    exec.countDown();
                    if (failExec) {
                        throw Status.INTERNAL.asRuntimeException();
                    }
                })
                .setOnStop(() -> {
                    stop.countDown();
                    if (failStop) {
                        throw Status.INTERNAL.asRuntimeException();
                    }
                })
                .build();
            servantUri = HostAndPort.fromParts("localhost", port);
        }
    }

    @NotNull
    private ServiceConfig buildConfig(int maxServants) {
        ServiceConfig config = new ServiceConfig();
        config.setMaxServantsPerWorkflow(maxServants);
        config.setDefaultProvisioningLimit(maxServants);
        config.setProvisioningLimits(Map.of());
        return config;
    }

    private SchedulerImpl createScheduler(int maxServants) {
        ServiceConfig config = buildConfig(maxServants);
        var processorConfig = new ProcessorConfigBuilder()
            .setIdleTimeout(100)
            .setConfiguringTimeout(100)
            .setExecutingHeartbeatPeriod(100)
            .setServantStopTimeout(100)
            .build();

        ServantsPool pool = new ServantsPoolImpl(config, processorConfig, servantDao, allocator, events, tasks,
            manager);
        SchedulerImpl scheduler = new SchedulerImpl(servantDao, tasks, pool, config);
        scheduler.start();

        return scheduler;
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
