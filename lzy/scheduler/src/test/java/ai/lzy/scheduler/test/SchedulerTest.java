package ai.lzy.scheduler.test;

import ai.lzy.model.ReturnCodes;
import ai.lzy.model.TaskDesc;
import ai.lzy.model.db.exceptions.DaoException;
import ai.lzy.model.db.test.DatabaseTestUtils;
import ai.lzy.model.utils.FreePortFinder;
import ai.lzy.scheduler.configs.ProcessorConfigBuilder;
import ai.lzy.scheduler.configs.ServiceConfig;
import ai.lzy.scheduler.db.TaskDao;
import ai.lzy.scheduler.db.SchedulerDataSource;
import ai.lzy.scheduler.models.TaskState;
import ai.lzy.scheduler.test.mocks.AllocatedWorkerMock;
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


public class SchedulerTest {

//    @Rule
//    public PreparedDbRule db = EmbeddedPostgresRules.preparedDatabase(ds -> {});
//
//    public ApplicationContext context;
//    public WorkerEventDao events;
//    public WorkerDao workerDao;
//    public TaskDao tasks;
//    public EventQueueManager manager;
//
//    public AllocatorMock allocator;
//    public String workflowId;
//    public String workflowName;
//    public String userId;
//
//    @Rule
//    public Timeout globalTimeout = Timeout.seconds(30);
//    private SchedulerDataSource storage;
//
//    @Before
//    public void setUp() {
//        context = ApplicationContext.run(DatabaseTestUtils.preparePostgresConfig("scheduler", db.getConnectionInfo()));
//
//        events = context.getBean(WorkerEventDao.class);
//        workerDao = context.getBean(WorkerDao.class);
//        tasks = context.getBean(TaskDao.class);
//        manager = context.getBean(EventQueueManager.class);
//        storage = context.getBean(SchedulerDataSource.class);
//
//        workflowId = "wfid" + UUID.randomUUID();
//        workflowName = "wf" + UUID.randomUUID();
//        userId = "uid" + UUID.randomUUID();
//        allocator = new AllocatorMock();
//    }
//
//    @After
//    public void tearDown() throws DaoException {
//        for (Worker worker : workerDao.get(workflowId)) {
//            workerDao.invalidate(worker, "destroy");
//            events.removeAll(worker.id());
//        }
//        for (var task : tasks.list(workflowId)) {
//            task.notifyExecutionCompleted(1, "End of test");
//        }
//        context.close();
//    }
//
//    @Test
//    public void testSimple() throws Exception {
//        ServiceConfig config = buildConfig(1);
//        var processorConfig = new ProcessorConfigBuilder()
//            .setIdleTimeout(100)
//            .build();
//
//        WorkersPool pool = new WorkersPoolImpl(config, processorConfig, workerDao, allocator, events, tasks,
//            manager, storage);
//        SchedulerImpl scheduler = new SchedulerImpl(workerDao, tasks, pool, config);
//        scheduler.start();
//        final CompletableFuture<AllocationRequest> allocationRequested = new CompletableFuture<>();
//        allocator.onAllocationRequested(((a, b, c) -> allocationRequested.complete(new AllocationRequest(a, b, c))));
//
//        scheduler.execute(workflowId, workflowName, userId, new TaskDesc(buildOp(), Map.of())); //task 1
//        var req = allocationRequested.get();
//
//        final var port = FreePortFinder.find(1000, 2000);
//        final BlockingQueue<String> env = new LinkedBlockingQueue<>();
//        final BlockingQueue<String> exec = new LinkedBlockingQueue<>();
//        //noinspection MismatchedQueryAndUpdateOfCollection
//        final BlockingQueue<String> stop = new LinkedBlockingQueue<>();
//        new AllocatedWorkerMock.WorkerBuilder(port)
//            .setOnEnv(() -> env.add(""))
//            .setOnExec(() -> exec.add(""))
//            .setOnStop(() -> stop.add(""))
//            .build();
//        final HostAndPort workerUri = HostAndPort.fromParts("localhost", port);
//        awaitState(req.workflowId(), req.workerId(), WorkerState.Status.CONNECTING);
//        var worker = workerDao.get(req.workflowId(), req.workerId());
//        Objects.requireNonNull(worker).notifyConnected(workerUri);
//
//        env.take();
//        worker.notifyConfigured(0, "Ok");
//
//        exec.take();
//        worker.notifyExecutionCompleted(0, "Ok");
//
//        scheduler.execute(workflowId, workflowName, userId, new TaskDesc(buildOp(), Map.of())); //task 2
//        scheduler.execute(workflowId, workflowName, userId, new TaskDesc(buildOp(), Map.of())); //task 3
//
//        env.take();
//        worker.notifyConfigured(0, "OK");
//
//        exec.take();
//        worker.notifyExecutionCompleted(0, "Ok");
//
//        env.take();
//        worker.notifyConfigured(0, "Ok");
//
//        exec.take();
//        worker.notifyExecutionCompleted(0, "Ok");
//
//        awaitState(req.workflowId(), req.workerId(), WorkerState.Status.RUNNING);
//        worker.notifyCommunicationCompleted();
//
//        awaitState(req.workflowId(), req.workerId(), WorkerState.Status.STOPPING);
//        worker.notifyStopped(0, "Stopped");
//
//        awaitState(req.workflowId(), req.workerId(), WorkerState.Status.DESTROYED);
//    }
//
//    @Test
//    public void testParallel() throws Exception {
//        SchedulerImpl scheduler = createScheduler(2);
//
//        final BlockingQueue<AllocationRequest> requests = new LinkedBlockingQueue<>();
//        allocator.onAllocationRequested(((a, b, c) -> requests.add(new AllocationRequest(a, b, c))));
//
//        scheduler.execute(workflowId, workflowName, userId, new TaskDesc(buildOp(), Map.of())); //task 1
//        scheduler.execute(workflowId, workflowName, userId, new TaskDesc(buildOp(), Map.of())); //task 2
//
//        final var state1 = new WorkerTestState();
//        final var state2 = new WorkerTestState();
//
//        var r1 = requests.take();
//        var worker1 = workerDao.get(r1.workflowId(), r1.workerId());
//        Objects.requireNonNull(worker1).notifyConnected(state1.workerUri);
//
//        var r2 = requests.take();
//        var worker2 = workerDao.get(r2.workflowId(), r2.workerId());
//        Objects.requireNonNull(worker2).notifyConnected(state2.workerUri);
//
//        state1.env.await();
//        state2.env.await();
//
//        worker1.notifyConfigured(0, "Ok");
//        worker2.notifyConfigured(0, "Ok");
//
//        state1.exec.await();
//        state2.exec.await();
//
//        worker1.notifyExecutionCompleted(0, "Ok");
//        worker2.notifyExecutionCompleted(0, "Ok");
//
//        state1.stop.await();
//        state2.stop.await();
//
//        worker1.notifyStopped(0, "Ok");
//        worker2.notifyStopped(0, "Ok");
//
//        awaitState(r1.workflowId(), r1.workerId(), WorkerState.Status.DESTROYED);
//        awaitState(r2.workflowId(), r2.workerId(), WorkerState.Status.DESTROYED);
//    }
//
//    @Test
//    public void testRestart() throws Exception {
//        ServiceConfig config = buildConfig(1);
//        var processorConfig = new ProcessorConfigBuilder()
//            .setIdleTimeout(100)
//            .build();
//
//        WorkersPool pool = new WorkersPoolImpl(config, processorConfig, workerDao, allocator, events, tasks,
//            manager, storage);
//        SchedulerImpl scheduler = new SchedulerImpl(workerDao, tasks, pool, config);
//        scheduler.start();
//
//        Function<SchedulerImpl, SchedulerImpl> restart = (sh) -> {
//            sh.terminate();
//            try {
//                sh.awaitTermination();
//            } catch (InterruptedException e) {
//                throw new RuntimeException(e);
//            }
//            var poolTmp = new WorkersPoolImpl(config, processorConfig, workerDao, allocator, events,
//                    tasks, manager, storage);
//            var schedulerTmp = new SchedulerImpl(workerDao, tasks, poolTmp, config);
//            schedulerTmp.start();
//            return schedulerTmp;
//        };
//
//        final CompletableFuture<AllocationRequest> allocationRequested = new CompletableFuture<>();
//        allocator.onAllocationRequested(((a, b, c) -> allocationRequested.complete(new AllocationRequest(a, b, c))));
//        scheduler.execute(workflowId, workflowName, userId, new TaskDesc(buildOp(), Map.of())); //task 1
//        var req = allocationRequested.get();
//
//        final var port = FreePortFinder.find(1000, 2000);
//        final BlockingQueue<String> env = new LinkedBlockingQueue<>();
//        final BlockingQueue<String> exec = new LinkedBlockingQueue<>();
//        //noinspection MismatchedQueryAndUpdateOfCollection
//        final BlockingQueue<String> stop = new LinkedBlockingQueue<>();
//
//        new AllocatedWorkerMock.WorkerBuilder(port)
//            .setOnEnv(() -> env.add(""))
//            .setOnExec(() -> exec.add(""))
//            .setOnStop(() -> stop.add(""))
//            .build();
//        final HostAndPort workerUri = HostAndPort.fromParts("localhost", port);
//        awaitState(req.workflowId(), req.workerId(), WorkerState.Status.CONNECTING);
//        var worker = workerDao.get(req.workflowId(), req.workerId());
//        Objects.requireNonNull(worker).notifyConnected(workerUri);
//
//        env.take();
//
//        scheduler = restart.apply(scheduler);
//
//        worker.notifyConfigured(0, "Ok");
//
//        exec.take();
//        worker.notifyExecutionCompleted(0, "Ok");
//
//        scheduler = restart.apply(scheduler);
//
//        scheduler.execute(workflowId, workflowName, userId, new TaskDesc(buildOp(), Map.of())); //task 2
//
//        scheduler = restart.apply(scheduler);
//
//        scheduler.execute(workflowId, workflowName, userId, new TaskDesc(buildOp(), Map.of())); //task 3
//
//        env.take();
//        worker.notifyConfigured(0, "OK");
//
//        scheduler = restart.apply(scheduler);
//
//        exec.take();
//        worker.notifyExecutionCompleted(0, "Ok");
//
//        scheduler = restart.apply(scheduler);
//
//        env.take();
//        worker.notifyConfigured(0, "Ok");
//
//        scheduler = restart.apply(scheduler);
//
//        exec.take();
//        worker.notifyExecutionCompleted(0, "Ok");
//
//        scheduler = restart.apply(scheduler);
//
//        awaitState(req.workflowId(), req.workerId(), WorkerState.Status.RUNNING);
//        worker.notifyCommunicationCompleted();
//
//        scheduler = restart.apply(scheduler);
//
//        awaitState(req.workflowId(), req.workerId(), WorkerState.Status.STOPPING);
//        worker.notifyStopped(0, "Stopped");
//
//        //noinspection UnusedAssignment
//        scheduler = restart.apply(scheduler);
//
//        awaitState(req.workflowId(), req.workerId(), WorkerState.Status.DESTROYED);
//    }
//
//    @Test
//    public void testFailOnEnv() throws Exception {
//        SchedulerImpl scheduler = createScheduler(1);
//
//        final CompletableFuture<AllocationRequest> allocationRequested = new CompletableFuture<>();
//        allocator.onAllocationRequested(((a, b, c) -> allocationRequested.complete(new AllocationRequest(a, b, c))));
//
//        scheduler.execute(workflowId, workflowName, userId, new TaskDesc(buildOp(), Map.of()));
//        /// WHEN
//        final var state1 = new WorkerTestState(true, false, false);
//
//        var req = allocationRequested.get();
//        awaitState(req.workflowId(), req.workerId(), WorkerState.Status.CONNECTING);
//        var worker = workerDao.get(req.workflowId(), req.workerId());
//        Objects.requireNonNull(worker).notifyConnected(state1.workerUri);
//
//        state1.env.await();
//
//        /// THEN
//        awaitState(req.workflowId(), req.workerId(), WorkerState.Status.DESTROYED);
//
//        for (var task : tasks.list(workflowId)) {
//            Assert.assertEquals(TaskState.Status.ERROR, task.status());
//            Assert.assertEquals("Internal error", task.errorDescription());
//        }
//    }
//
//    @Test
//    public void testFailOnExec() throws Exception {
//        SchedulerImpl scheduler = createScheduler(1);
//
//        final CompletableFuture<AllocationRequest> allocationRequested = new CompletableFuture<>();
//        allocator.onAllocationRequested(((a, b, c) -> allocationRequested.complete(new AllocationRequest(a, b, c))));
//
//        scheduler.execute(workflowId, workflowName, userId, new TaskDesc(buildOp(), Map.of()));
//        /// WHEN
//        final var state1 = new WorkerTestState(false, true, false);
//
//        var req = allocationRequested.get();
//        awaitState(req.workflowId(), req.workerId(), WorkerState.Status.CONNECTING);
//        var worker = workerDao.get(req.workflowId(), req.workerId());
//        Objects.requireNonNull(worker).notifyConnected(state1.workerUri);
//
//        state1.env.await();
//        worker.notifyConfigured(0, "Ok");
//        state1.exec.await();
//
//        /// THEN
//        awaitState(req.workflowId(), req.workerId(), WorkerState.Status.DESTROYED);
//
//        for (var task : tasks.list(workflowId)) {
//            Assert.assertEquals(TaskState.Status.ERROR, task.status());
//            Assert.assertEquals("Internal error", task.errorDescription());
//        }
//    }
//
//    @Test
//    public void testFailOnStop() throws Exception {
//        SchedulerImpl scheduler = createScheduler(1);
//
//        final CompletableFuture<AllocationRequest> allocationRequested = new CompletableFuture<>();
//        allocator.onAllocationRequested(((a, b, c) -> allocationRequested.complete(new AllocationRequest(a, b, c))));
//
//        scheduler.execute(workflowId, workflowName, userId, new TaskDesc(buildOp(), Map.of()));
//        /// WHEN
//        final var state1 = new WorkerTestState(false, false, true);
//
//        var req = allocationRequested.get();
//        awaitState(req.workflowId(), req.workerId(), WorkerState.Status.CONNECTING);
//        var worker = workerDao.get(req.workflowId(), req.workerId());
//        Objects.requireNonNull(worker).notifyConnected(state1.workerUri);
//
//        state1.env.await();
//        worker.notifyConfigured(0, "Ok");
//        state1.exec.await();
//        worker.notifyExecutionCompleted(0, "Ok");
//        state1.stop.await();
//
//        /// THEN
//        awaitState(req.workflowId(), req.workerId(), WorkerState.Status.DESTROYED);
//
//        for (var task : tasks.list(workflowId)) {
//            Assert.assertEquals(TaskState.Status.SUCCESS, task.status());
//        }
//    }
//
//    @Test
//    public void testConfigurationTimeout() throws Exception {
//        SchedulerImpl scheduler = createScheduler(1);
//
//        final CompletableFuture<AllocationRequest> allocationRequested = new CompletableFuture<>();
//        allocator.onAllocationRequested(((a, b, c) -> allocationRequested.complete(new AllocationRequest(a, b, c))));
//
//        scheduler.execute(workflowId, workflowName, userId, new TaskDesc(buildOp(), Map.of()));
//        final var state1 = new WorkerTestState();
//
//        var req = allocationRequested.get();
//        awaitState(req.workflowId(), req.workerId(), WorkerState.Status.CONNECTING);
//        var worker = workerDao.get(req.workflowId(), req.workerId());
//        Objects.requireNonNull(worker).notifyConnected(state1.workerUri);
//
//        state1.env.await();
//        /// WHEN - not notified
//
//        /// THEN
//        awaitState(req.workflowId(), req.workerId(), WorkerState.Status.DESTROYED);
//
//        for (var task : tasks.list(workflowId)) {
//            Assert.assertEquals(TaskState.Status.ERROR, task.status());
//            Assert.assertEquals("Worker stopping: <Environment is installing too long.>", task.errorDescription());
//        }
//    }
//    @Test
//    public void testExecutionTimeout() throws Exception {
//        SchedulerImpl scheduler = createScheduler(1);
//
//        final CompletableFuture<AllocationRequest> allocationRequested = new CompletableFuture<>();
//        allocator.onAllocationRequested(((a, b, c) -> allocationRequested.complete(new AllocationRequest(a, b, c))));
//
//        scheduler.execute(workflowId, workflowName, userId, new TaskDesc(buildOp(), Map.of()));
//        final var state1 = new WorkerTestState();
//
//        var req = allocationRequested.get();
//        awaitState(req.workflowId(), req.workerId(), WorkerState.Status.CONNECTING);
//        var worker = workerDao.get(req.workflowId(), req.workerId());
//        Objects.requireNonNull(worker).notifyConnected(state1.workerUri);
//
//        state1.env.await();
//        worker.notifyConfigured(0, "Ok");
//        state1.exec.await();
//        worker.executingHeartbeat();
//        /// WHEN - not notified
//
//        /// THEN
//        awaitState(req.workflowId(), req.workerId(), WorkerState.Status.DESTROYED);
//
//        for (var task : tasks.list(workflowId)) {
//            Assert.assertEquals(TaskState.Status.ERROR, task.status());
//            Assert.assertEquals("Worker is dead", task.errorDescription());
//        }
//    }
//
//    @Test
//    public void testStopTimeout() throws Exception {
//        SchedulerImpl scheduler = createScheduler(1);
//
//        final CompletableFuture<AllocationRequest> allocationRequested = new CompletableFuture<>();
//        allocator.onAllocationRequested(((a, b, c) -> allocationRequested.complete(new AllocationRequest(a, b, c))));
//
//        scheduler.execute(workflowId, workflowName, userId, new TaskDesc(buildOp(), Map.of()));
//        final var state1 = new WorkerTestState();
//
//        var req = allocationRequested.get();
//        awaitState(req.workflowId(), req.workerId(), WorkerState.Status.CONNECTING);
//        var worker = workerDao.get(req.workflowId(), req.workerId());
//        Objects.requireNonNull(worker).notifyConnected(state1.workerUri);
//
//        state1.env.await();
//        worker.notifyConfigured(0, "Ok");
//        state1.exec.await();
//        worker.notifyExecutionCompleted(0, "Ok");
//        state1.stop.await();
//        /// WHEN - not notified
//
//        /// THEN
//        awaitState(req.workflowId(), req.workerId(), WorkerState.Status.DESTROYED);
//
//        for (var task : tasks.list(workflowId)) {
//            Assert.assertEquals(TaskState.Status.SUCCESS, task.status());
//        }
//    }
//
//    @Test
//    public void testFailOnEnvNotifying() throws Exception {
//        SchedulerImpl scheduler = createScheduler(1);
//
//        final CompletableFuture<AllocationRequest> allocationRequested = new CompletableFuture<>();
//        allocator.onAllocationRequested(((a, b, c) -> allocationRequested.complete(new AllocationRequest(a, b, c))));
//
//        scheduler.execute(workflowId, workflowName, userId, new TaskDesc(buildOp(), Map.of()));
//        final var state1 = new WorkerTestState();
//
//        var req = allocationRequested.get();
//        awaitState(req.workflowId(), req.workerId(), WorkerState.Status.CONNECTING);
//        var worker = workerDao.get(req.workflowId(), req.workerId());
//        Objects.requireNonNull(worker).notifyConnected(state1.workerUri);
//
//        state1.env.await();
//
//        /// WHEN
//        worker.notifyConfigured(ReturnCodes.ENVIRONMENT_INSTALLATION_ERROR.getRc(), "test");
//
//        /// THEN
//        awaitState(req.workflowId(), req.workerId(), WorkerState.Status.STOPPING);
//        worker.notifyStopped(0, "Stopped");
//        awaitState(req.workflowId(), req.workerId(), WorkerState.Status.DESTROYED);
//
//        for (var task : tasks.list(workflowId)) {
//            Assert.assertEquals(TaskState.Status.ERROR, task.status());
//            Assert.assertEquals("Error while configuring worker: test", task.errorDescription());
//        }
//    }
//
//    @Test
//    public void testFailOnExecNotifying() throws Exception {
//        SchedulerImpl scheduler = createScheduler(1);
//
//        final CompletableFuture<AllocationRequest> allocationRequested = new CompletableFuture<>();
//        allocator.onAllocationRequested(((a, b, c) -> allocationRequested.complete(new AllocationRequest(a, b, c))));
//
//        scheduler.execute(workflowId, workflowName, userId, new TaskDesc(buildOp(), Map.of()));
//        final var state1 = new WorkerTestState();
//
//        var req = allocationRequested.get();
//        awaitState(req.workflowId(), req.workerId(), WorkerState.Status.CONNECTING);
//        var worker = workerDao.get(req.workflowId(), req.workerId());
//        Objects.requireNonNull(worker).notifyConnected(state1.workerUri);
//
//        state1.env.await();
//        worker.notifyConfigured(0, "Ok");
//        state1.exec.await();
//
//        /// WHEN
//        worker.notifyExecutionCompleted(ReturnCodes.ENVIRONMENT_INSTALLATION_ERROR.getRc(), "test");
//
//        /// THEN
//        awaitState(req.workflowId(), req.workerId(), WorkerState.Status.STOPPING);
//        worker.notifyStopped(0, "Stopped");
//        awaitState(req.workflowId(), req.workerId(), WorkerState.Status.DESTROYED);
//
//        for (var task : tasks.list(workflowId)) {
//            Assert.assertEquals(TaskState.Status.ERROR, task.status());
//            Assert.assertEquals("test", task.errorDescription());
//        }
//    }
//
//    @Test
//    public void testFailOnStopNotifying() throws Exception {
//        SchedulerImpl scheduler = createScheduler(1);
//
//        final CompletableFuture<AllocationRequest> allocationRequested = new CompletableFuture<>();
//        allocator.onAllocationRequested(((a, b, c) -> allocationRequested.complete(new AllocationRequest(a, b, c))));
//
//        scheduler.execute(workflowId, workflowName, userId, new TaskDesc(buildOp(), Map.of()));
//        final var state1 = new WorkerTestState();
//
//        var req = allocationRequested.get();
//        awaitState(req.workflowId(), req.workerId(), WorkerState.Status.CONNECTING);
//        var worker = workerDao.get(req.workflowId(), req.workerId());
//        Objects.requireNonNull(worker).notifyConnected(state1.workerUri);
//
//        state1.env.await();
//        worker.notifyConfigured(0, "Ok");
//        state1.exec.await();
//        worker.notifyExecutionCompleted(0, "Ok");
//        state1.stop.await();
//
//        /// WHEN
//        worker.notifyStopped(ReturnCodes.ENVIRONMENT_INSTALLATION_ERROR.getRc(), "test");
//
//        /// THEN
//        awaitState(req.workflowId(), req.workerId(), WorkerState.Status.DESTROYED);
//
//        for (var task : tasks.list(workflowId)) {
//            Assert.assertEquals(TaskState.Status.SUCCESS, task.status());
//        }
//    }
//
//    @Test
//    public void testFailingWorkerDontAffectOther() throws Exception {
//        SchedulerImpl scheduler = createScheduler(2);
//
//        final BlockingQueue<AllocationRequest> requests = new LinkedBlockingQueue<>();
//        allocator.onAllocationRequested(((a, b, c) -> requests.add(new AllocationRequest(a, b, c))));
//
//        var workflowId2 = "wfid" + UUID.randomUUID();
//        var workflowName2 = "wf" + UUID.randomUUID();
//        scheduler.execute(workflowId, workflowName, userId, new TaskDesc(buildOp(), Map.of())); //task 1
//        scheduler.execute(workflowId2, workflowName2, userId, new TaskDesc(buildOp(), Map.of())); //task 2
//
//        /// WHEN
//        final var state1 = new WorkerTestState(true, false, false);
//        final var state2 = new WorkerTestState();
//
//        var r1 = requests.take();
//        var worker1 = workerDao.get(r1.workflowId(), r1.workerId());
//        Objects.requireNonNull(worker1).notifyConnected(state1.workerUri);
//
//        var r2 = requests.take();
//        var worker2 = workerDao.get(r2.workflowId(), r2.workerId());
//        Objects.requireNonNull(worker2).notifyConnected(state2.workerUri);
//
//        state1.env.await();
//        state2.env.await();
//        worker2.notifyConfigured(0, "Ok");
//
//        state2.exec.await();
//        worker2.notifyExecutionCompleted(0, "Ok");
//
//        awaitState(r2.workflowId(), r2.workerId(), WorkerState.Status.RUNNING);
//        worker2.notifyCommunicationCompleted();
//
//        awaitState(r2.workflowId(), r2.workerId(), WorkerState.Status.STOPPING);
//        worker2.notifyStopped(0, "Stopped");
//
//        awaitState(r2.workflowId(), r2.workerId(), WorkerState.Status.DESTROYED);
//
//        /// THEN
//        awaitState(r1.workflowId(), r1.workerId(), WorkerState.Status.DESTROYED);
//
//        for (var task : tasks.list(r1.workflowId())) {
//            Assert.assertEquals(TaskState.Status.ERROR, task.status());
//            Assert.assertEquals("Internal error", task.errorDescription());
//        }
//
//        for (var task : tasks.list(r2.workflowId())) {
//            Assert.assertEquals(TaskState.Status.SUCCESS, task.status());
//        }
//    }
//
//    private static class WorkerTestState {
//        final CountDownLatch env = new CountDownLatch(1);
//        final CountDownLatch exec = new CountDownLatch(1);
//        final CountDownLatch stop = new CountDownLatch(1);
//        final HostAndPort workerUri;
//
//        public WorkerTestState() throws IOException {
//            this(false, false, false);
//        }
//
//        public WorkerTestState(boolean failEnv, boolean failExec, boolean failStop) throws IOException {
//            final var port = FreePortFinder.find(1000, 2000);
//            new AllocatedWorkerMock.WorkerBuilder(port)
//                .setOnEnv(() -> {
//                    env.countDown();
//                    if (failEnv) {
//                        throw Status.INTERNAL.asRuntimeException();
//                    }
//                })
//                .setOnExec(() -> {
//                    exec.countDown();
//                    if (failExec) {
//                        throw Status.INTERNAL.asRuntimeException();
//                    }
//                })
//                .setOnStop(() -> {
//                    stop.countDown();
//                    if (failStop) {
//                        throw Status.INTERNAL.asRuntimeException();
//                    }
//                })
//                .build();
//            workerUri = HostAndPort.fromParts("localhost", port);
//        }
//    }
//
//    @NotNull
//    private ServiceConfig buildConfig(int maxWorkers) {
//        ServiceConfig config = new ServiceConfig();
//        config.setMaxWorkersPerWorkflow(maxWorkers);
//        config.setDefaultProvisioningLimit(maxWorkers);
//        config.setProvisioningLimits(Map.of());
//        return config;
//    }
//
//    private SchedulerImpl createScheduler(int maxWorkers) {
//        ServiceConfig config = buildConfig(maxWorkers);
//        var processorConfig = new ProcessorConfigBuilder()
//            .setIdleTimeout(100)
//            .setConfiguringTimeout(100)
//            .setExecutingHeartbeatPeriod(100)
//            .setWorkerStopTimeout(100)
//            .build();
//
//        WorkersPool pool = new WorkersPoolImpl(config, processorConfig, workerDao, allocator, events, tasks,
//            manager, storage);
//        SchedulerImpl scheduler = new SchedulerImpl(workerDao, tasks, pool, config);
//        scheduler.start();
//
//        return scheduler;
//    }
//
//    public void awaitState(String workflowId, String workerId,
//                           WorkerState.Status status) throws InterruptedException, DaoException
//    {
//        WorkerState.Status s = null;
//        var worker = workerDao.get(workflowId, workerId);
//        if (worker != null) {
//            s = worker.status();
//        }
//        while (s == null || s != status) {
//            Thread.sleep(10);
//            worker = workerDao.get(workflowId, workerId);
//            if (worker == null) {
//                s = null;
//            } else {
//                s = worker.status();
//            }
//        }
//    }
}
