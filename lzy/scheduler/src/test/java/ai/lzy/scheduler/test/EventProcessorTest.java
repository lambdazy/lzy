package ai.lzy.scheduler.test;

import ai.lzy.model.TaskDesc;
import ai.lzy.model.db.exceptions.DaoException;
import ai.lzy.model.db.test.DatabaseTestUtils;
import ai.lzy.model.operation.Operation;
import ai.lzy.model.utils.FreePortFinder;
import ai.lzy.scheduler.configs.ProcessorConfigBuilder;
import ai.lzy.scheduler.configs.WorkerEventProcessorConfig;
import ai.lzy.scheduler.db.TaskDao;
import ai.lzy.scheduler.test.mocks.AllocatedWorkerMock;
import ai.lzy.scheduler.test.mocks.AllocatorMock;
import com.google.common.net.HostAndPort;
import io.grpc.Status;
import io.micronaut.context.ApplicationContext;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

public class EventProcessorTest {
//    private static final Logger LOG = LogManager.getLogger(EventProcessorTest.class);
//
//    @Rule
//    public PreparedDbRule db = EmbeddedPostgresRules.preparedDatabase(ds -> {
//    });
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
//    public CountDownLatch workerReady;
//
//    @Rule
//    public Timeout globalTimeout = Timeout.seconds(10);
//
//    @Before
//    public void setUp() {
//        context = ApplicationContext.run(DatabaseTestUtils.preparePostgresConfig("scheduler", db.getConnectionInfo()));
//
//        events = context.getBean(WorkerEventDao.class);
//        workerDao = context.getBean(WorkerDao.class);
//        tasks = context.getBean(TaskDao.class);
//        manager = context.getBean(EventQueueManager.class);
//
//        workflowId = "wfid" + UUID.randomUUID();
//        workflowName = "wf" + UUID.randomUUID();
//        userId = "uid" + UUID.randomUUID();
//        allocator = new AllocatorMock();
//        workerReady = new CountDownLatch(1);
//    }
//
//    @After
//    public void tearDown() throws DaoException {
//        for (Worker worker : workerDao.get(workflowName)) {
//            workerDao.invalidate(worker, "destroy");
//            events.removeAll(worker.id());
//        }
//        for (var task : tasks.list(workflowId)) {
//            task.notifyExecutionCompleted(1, "End of test");
//        }
//
//        context.close();
//    }
//
//    @Test(timeout = 1000)
//    public void testAwaitState() throws Exception {
//        var s = workerDao.create(userId, workflowName, new Operation.Requirements("s", "a"));
//        var t = new Thread(() -> {
//            try {
//                awaitState(s.workflowName(), s.id(), WorkerState.Status.DESTROYED);
//            } catch (InterruptedException | DaoException e) {
//                LOG.error(e);
//                throw new RuntimeException("e");
//            }
//        });
//        t.start();
//        workerDao.invalidate(s, "Lol");
//        LOG.info("Updated");
//        t.join();
//    }
//
//    @Test
//    public void testSimple() throws Exception {
//        var config = new ProcessorConfigBuilder()
//            .setIdleTimeout(100)
//            .build();
//        try (var processor = new ProcessorContext(config)) {
//            final CompletableFuture<AllocationRequest> allocationRequested = new CompletableFuture<>();
//            allocator.onAllocationRequested(
//                ((a, b, c) -> allocationRequested.complete(new AllocationRequest(a, b, c))));
//            final var task = processor.generateTask();
//            processor.getWorker().setTask(task);
//            allocationRequested.get();
//            processor.getWorker().notifyConnected(processor.generateWorker());
//            processor.env.await();
//            processor.worker.notifyConfigured(0, "Ok");
//            processor.exec.await();
//            processor.worker.notifyExecutionCompleted(0, "Ok");
//            awaitState(processor.worker.workflowName(), processor.worker.id(), WorkerState.Status.RUNNING);
//
//            final var newTask = tasks.get(task.taskId());
//            Assert.assertNotNull(newTask);
//            Assert.assertEquals("Ok", newTask.errorDescription());
//            Assert.assertEquals(0, Objects.requireNonNull(newTask.rc()).intValue());
//            final var worker = workerDao.get(processor.worker.workflowName(), processor.worker.id());
//            Assert.assertNull(Objects.requireNonNull(worker).taskId());
//
//            processor.worker.notifyCommunicationCompleted();
//            awaitState(processor.worker.workflowName(), processor.worker.id(), WorkerState.Status.IDLE);
//            processor.stop.await(); // Idle timeout
//            processor.worker.notifyStopped(0, "Ok");
//            awaitState(processor.worker.workflowName(),
//                processor.worker.id(), WorkerState.Status.DESTROYED);  //  Destroyed after stop
//        }
//    }
//
//    @Test
//    public void testAllocationTimeout() throws Exception {
//        var config = new ProcessorConfigBuilder()
//            .setAllocationTimeout(100)
//            .build();
//        try (var processor = new ProcessorContext(config)) {
//            final var task = processor.generateTask();
//            processor.getWorker().setTask(task);
//            awaitState(processor.worker.workflowName(),
//                processor.worker.id(), WorkerState.Status.DESTROYED);
//        }
//    }
//
//    @Test
//    public void testIdleTimeout() throws Exception {
//        var config = new ProcessorConfigBuilder()
//            .setIdleTimeout(100)
//            .setWorkerStopTimeout(100)
//            .build();
//        try (var processor = new ProcessorContext(config)) {
//            final CompletableFuture<AllocationRequest> allocationRequested = new CompletableFuture<>();
//            allocator.onAllocationRequested(
//                ((a, b, c) -> allocationRequested.complete(new AllocationRequest(a, b, c))));
//            final var task = processor.generateTask();
//            processor.getWorker().setTask(task);
//            allocationRequested.get();
//            processor.getWorker().notifyConnected(processor.generateWorker());
//            processor.env.await();
//            processor.worker.notifyConfigured(0, "Ok");
//            processor.exec.await();
//            processor.worker.notifyExecutionCompleted(0, "Ok");
//            awaitState(processor.worker.workflowName(),
//                processor.worker.id(), WorkerState.Status.DESTROYED);
//        }
//    }
//
//    @Test
//    public void testConfigurationTimeout() throws Exception {
//        var config = new ProcessorConfigBuilder()
//            .setIdleTimeout(100)
//            .setWorkerStopTimeout(100)
//            .setConfiguringTimeout(100)
//            .build();
//        try (var processor = new ProcessorContext(config)) {
//            final CompletableFuture<AllocationRequest> allocationRequested = new CompletableFuture<>();
//            allocator.onAllocationRequested(
//                ((a, b, c) -> allocationRequested.complete(new AllocationRequest(a, b, c))));
//            final var task = processor.generateTask();
//            processor.getWorker().setTask(task);
//            allocationRequested.get();
//            processor.getWorker().notifyConnected(processor.generateWorker());
//            awaitState(processor.worker.workflowName(),
//                processor.worker.id(), WorkerState.Status.DESTROYED);
//        }
//    }
//
//    @Test
//    public void testStoppingTimeout() throws Exception {
//        var config = new ProcessorConfigBuilder()
//            .setIdleTimeout(100)
//            .setWorkerStopTimeout(100)
//            .build();
//        try (var processor = new ProcessorContext(config)) {
//            final CompletableFuture<AllocationRequest> allocationRequested = new CompletableFuture<>();
//            allocator.onAllocationRequested(
//                ((a, b, c) -> allocationRequested.complete(new AllocationRequest(a, b, c))));
//            final var task = processor.generateTask();
//            processor.getWorker().setTask(task);
//            allocationRequested.get();
//            processor.getWorker().notifyConnected(processor.generateWorker());
//            processor.env.await();
//            processor.worker.notifyConfigured(0, "Ok");
//            processor.worker.stop("Test");
//            awaitState(processor.worker.workflowName(),
//                processor.worker.id(), WorkerState.Status.DESTROYED);
//        }
//    }
//
//    @Test
//    public void testExecutingHeartbeats() throws Exception {
//        var config = new ProcessorConfigBuilder()
//            .setExecutingHeartbeatPeriod(100)
//            .build();
//        try (var processor = new ProcessorContext(config)) {
//            final CompletableFuture<AllocationRequest> allocationRequested = new CompletableFuture<>();
//            allocator.onAllocationRequested(
//                ((a, b, c) -> allocationRequested.complete(new AllocationRequest(a, b, c))));
//            final var task = processor.generateTask();
//            processor.getWorker().setTask(task);
//            allocationRequested.get();
//            processor.getWorker().notifyConnected(processor.generateWorker());
//            processor.env.await();
//            processor.worker.notifyConfigured(0, "Ok");
//            processor.worker.executingHeartbeat();
//
//            Thread.sleep(50);
//            processor.worker.executingHeartbeat();
//            Thread.sleep(50);
//            processor.worker.executingHeartbeat();
//            Thread.sleep(50);
//            processor.worker.executingHeartbeat();
//            Thread.sleep(50);
//            processor.worker.executingHeartbeat();
//            awaitState(processor.worker.workflowName(),
//                processor.worker.id(), WorkerState.Status.DESTROYED);
//        }
//    }
//
//    @Test
//    public void testIdleHeartbeats() throws Exception {
//        var config = new ProcessorConfigBuilder()
//            .setIdleHeartbeatPeriod(100)
//            .build();
//        try (var processor = new ProcessorContext(config)) {
//            final CompletableFuture<AllocationRequest> allocationRequested = new CompletableFuture<>();
//            allocator.onAllocationRequested(
//                ((a, b, c) -> allocationRequested.complete(new AllocationRequest(a, b, c))));
//            final var task = processor.generateTask();
//            processor.getWorker().setTask(task);
//            allocationRequested.get();
//            processor.getWorker().notifyConnected(processor.generateWorker());
//            processor.env.await();
//            processor.worker.notifyConfigured(0, "Ok");
//            processor.exec.await();
//            processor.worker.notifyExecutionCompleted(0, "Ok");
//            Thread.sleep(50);
//            processor.worker.idleHeartbeat();
//            Thread.sleep(50);
//            processor.worker.idleHeartbeat();
//            Thread.sleep(50);
//            processor.worker.idleHeartbeat();
//            Thread.sleep(50);
//            processor.worker.idleHeartbeat();
//            awaitState(processor.worker.workflowName(),
//                processor.worker.id(), WorkerState.Status.DESTROYED);
//        }
//    }
//
//    @Test
//    public void testFailEnv() throws Exception {
//        var config = new ProcessorConfigBuilder()
//            .setIdleTimeout(100)
//            .setWorkerStopTimeout(100)
//            .build();
//        try (var processor = new ProcessorContext(config)) {
//            final CompletableFuture<AllocationRequest> allocationRequested = new CompletableFuture<>();
//            allocator.onAllocationRequested(
//                ((a, b, c) -> allocationRequested.complete(new AllocationRequest(a, b, c))));
//            final var task = processor.generateTask();
//            processor.getWorker().setTask(task);
//            allocationRequested.get();
//            final var workerURI = processor.generateWorker(/*failEnv*/ true, false, false);
//            processor.getWorker().notifyConnected(workerURI);
//            processor.env.await();
//            awaitState(processor.worker.workflowName(),
//                processor.worker.id(), WorkerState.Status.DESTROYED);
//        }
//    }
//
//    @Test
//    public void testFailExec() throws Exception {
//        var config = new ProcessorConfigBuilder()
//            .setIdleTimeout(100)
//            .setWorkerStopTimeout(100)
//            .build();
//        try (var processor = new ProcessorContext(config)) {
//            final CompletableFuture<AllocationRequest> allocationRequested = new CompletableFuture<>();
//            allocator.onAllocationRequested(
//                ((a, b, c) -> allocationRequested.complete(new AllocationRequest(a, b, c))));
//            final var task = processor.generateTask();
//            processor.getWorker().setTask(task);
//            allocationRequested.get();
//            final var workerURI = processor.generateWorker(false, /*failExec*/ true, false);
//            processor.getWorker().notifyConnected(workerURI);
//            processor.env.await();
//            processor.worker.notifyConfigured(0, "Ok");
//            processor.exec.await();
//            awaitState(processor.worker.workflowName(),
//                processor.worker.id(), WorkerState.Status.DESTROYED);  //  Destroyed by timeout
//        }
//    }
//
//    @Test
//    public void testFailStop() throws Exception {
//        var config = new ProcessorConfigBuilder()
//            .setIdleTimeout(100)
//            .setWorkerStopTimeout(100)
//            .build();
//        try (var processor = new ProcessorContext(config)) {
//            final CompletableFuture<AllocationRequest> allocationRequested = new CompletableFuture<>();
//            allocator.onAllocationRequested(
//                ((a, b, c) -> allocationRequested.complete(new AllocationRequest(a, b, c))));
//            final var task = processor.generateTask();
//            processor.getWorker().setTask(task);
//            allocationRequested.get();
//            final var workerURI = processor.generateWorker(false, false, /*failStop*/ true);
//            processor.worker.notifyConnected(workerURI);
//            processor.env.await();
//            processor.worker.notifyConfigured(0, "Ok");
//            processor.exec.await();
//            processor.worker.notifyExecutionCompleted(0, "Ok");
//            awaitState(processor.worker.workflowName(), processor.worker.id(), WorkerState.Status.RUNNING);
//
//            processor.worker.notifyCommunicationCompleted();
//            awaitState(processor.worker.workflowName(), processor.worker.id(), WorkerState.Status.IDLE);
//            awaitState(processor.worker.workflowName(),
//                processor.worker.id(), WorkerState.Status.DESTROYED);  //  Destroyed by timeout
//        }
//    }
//
//    @Test
//    public void testRestore() throws Exception {
//        var config = new ProcessorConfigBuilder()
//            .setIdleTimeout(100)
//            .build();
//        String workerId;
//        final CompletableFuture<AllocationRequest> allocationRequested;
//
//        try (var processor = new ProcessorContext(config)) {
//            workerId = processor.worker.id();
//            allocationRequested = new CompletableFuture<>();
//            allocator.onAllocationRequested(
//                ((a, b, c) -> allocationRequested.complete(new AllocationRequest(a, b, c))));
//            final var task = processor.generateTask();
//            processor.getWorker().setTask(task);
//        }
//
//        final var port = FreePortFinder.find(1000, 2000);
//        final CountDownLatch env = new CountDownLatch(1);
//        final CountDownLatch exec = new CountDownLatch(1);
//        final CountDownLatch stop = new CountDownLatch(1);
//
//        final var mock = new AllocatedWorkerMock.WorkerBuilder(port)
//            .setOnEnv(env::countDown)
//            .setOnExec(exec::countDown)
//            .setOnStop(stop::countDown)
//            .build();
//
//        try (var processor = new ProcessorContext(workerId, config)) {
//            allocationRequested.get();
//            awaitState(processor.worker.workflowName(), processor.worker.id(),
//                WorkerState.Status.CONNECTING);
//            processor.worker.notifyConnected(HostAndPort.fromParts("localhost", port));
//        }
//
//        try (var processor = new ProcessorContext(workerId, config)) {
//            env.await();
//            processor.worker.notifyConfigured(0, "Ok");
//        }
//        try (var processor = new ProcessorContext(workerId, config)) {
//            exec.await();
//            processor.worker.notifyExecutionCompleted(0, "Ok");
//        }
//        try (var processor = new ProcessorContext(workerId, config)) {
//            awaitState(processor.worker.workflowName(), processor.worker.id(), WorkerState.Status.RUNNING);
//            processor.worker.notifyCommunicationCompleted();
//        }
//        try (var processor = new ProcessorContext(workerId, config)) {
//            awaitState(processor.worker.workflowName(), processor.worker.id(), WorkerState.Status.IDLE);
//            awaitState(processor.worker.workflowName(),
//                processor.worker.id(), WorkerState.Status.STOPPING);  // Idle timeout
//            processor.worker.notifyStopped(0, "Ok");
//        }
//        try (var processor = new ProcessorContext(workerId, config)) {
//            awaitState(processor.worker.workflowName(),
//                processor.worker.id(), WorkerState.Status.DESTROYED);  //  Destroyed after stop
//        }
//
//        mock.close();
//    }
//
//    record AllocationRequest(String workflowId, String workerId, String token) {
//
//    }
//
//    public static Operation buildOp(String... tags) {
//        return new Operation(null, new Operation.Requirements("", ""), "", List.of(), "", "", null, null);
//    }
//
//    public class ProcessorContext implements AutoCloseable {
//
//        private final Worker worker;
//        private final WorkerEventProcessor processor;
//        private final CountDownLatch latch = new CountDownLatch(1);
//        private final String[] tags;
//        private final CountDownLatch env = new CountDownLatch(1);
//        private final CountDownLatch exec = new CountDownLatch(1);
//        private final CountDownLatch stop = new CountDownLatch(1);
//        private AllocatedWorkerMock mock;
//
//        public ProcessorContext(WorkerEventProcessorConfig config, String... provisioningTags) throws DaoException {
//            worker = workerDao.create(userId, workflowName, new Operation.Requirements("s", "a"));
//            processor = new WorkerEventProcessor(workflowName, worker.id(), config, allocator, tasks, events,
//                workerDao, manager, (a, b) -> latch.countDown(), (a, b) -> {
//            });
//            processor.start();
//            tags = provisioningTags;
//        }
//
//        public ProcessorContext(String workerId,
//                                WorkerEventProcessorConfig config, String... provisioningTags) throws DaoException
//        {
//            worker = Objects.requireNonNull(workerDao.get(workflowName, workerId));
//            processor = new WorkerEventProcessor(workflowName, workerId, config, allocator, tasks, events,
//                workerDao, manager, (a, b) -> latch.countDown(), (a, b) -> {
//            });
//            processor.start();
//            tags = provisioningTags;
//        }
//
//        public Task generateTask() throws DaoException {
//            return tasks.create(workflowId, workflowName, userId, new TaskDesc(buildOp(tags), Map.of()));
//        }
//
//        public HostAndPort generateWorker() throws IOException {
//            return generateWorker(false, false, false);
//        }
//
//        public HostAndPort generateWorker(boolean failEnv, boolean failExec, boolean failStop) throws IOException {
//            final var port = FreePortFinder.find(10000, 20000);
//            mock = new AllocatedWorkerMock.WorkerBuilder(port)
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
//            return HostAndPort.fromParts("localhost", port);
//        }
//
//        public Worker getWorker() {
//            return worker;
//        }
//
//        @Override
//        public void close() throws InterruptedException {
//            processor.shutdown();
//            processor.join();
//            if (mock != null) {
//                mock.close();
//            }
//        }
//    }
//
//    public void awaitState(String workflowName, String workerId,
//                           WorkerState.Status status) throws InterruptedException, DaoException
//    {
//        WorkerState.Status s = null;
//        var worker = workerDao.get(workflowName, workerId);
//        if (worker != null) {
//            s = worker.status();
//        }
//        while (s == null || s != status) {
//            LOG.debug("Got status {}, awaiting {}", s, status);
//            Thread.sleep(10);
//            worker = workerDao.get(workflowName, workerId);
//            if (worker == null) {
//                s = null;
//            } else {
//                s = worker.status();
//            }
//        }
//    }
}
