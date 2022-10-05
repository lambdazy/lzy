package ai.lzy.scheduler.test;

import ai.lzy.model.TaskDesc;
import ai.lzy.model.db.exceptions.DaoException;
import ai.lzy.model.db.test.DatabaseTestUtils;
import ai.lzy.model.operation.Operation;
import ai.lzy.model.utils.FreePortFinder;
import ai.lzy.scheduler.configs.ProcessorConfigBuilder;
import ai.lzy.scheduler.configs.ServantEventProcessorConfig;
import ai.lzy.scheduler.db.ServantDao;
import ai.lzy.scheduler.db.ServantEventDao;
import ai.lzy.scheduler.db.TaskDao;
import ai.lzy.scheduler.models.ServantState;
import ai.lzy.scheduler.servant.Servant;
import ai.lzy.scheduler.servant.impl.EventQueueManager;
import ai.lzy.scheduler.servant.impl.ServantEventProcessor;
import ai.lzy.scheduler.task.Task;
import ai.lzy.scheduler.test.mocks.AllocatedServantMock;
import ai.lzy.scheduler.test.mocks.AllocatorMock;
import io.grpc.Status;
import io.micronaut.context.ApplicationContext;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import org.apache.curator.shaded.com.google.common.net.HostAndPort;
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
    private static final Logger LOG = LogManager.getLogger(EventProcessorTest.class);

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
    public CountDownLatch servantReady;

    @Rule
    public Timeout globalTimeout = Timeout.seconds(10);

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
        servantReady = new CountDownLatch(1);
    }

    @After
    public void tearDown() throws DaoException {
        for (Servant servant : servantDao.get(workflowName)) {
            servantDao.invalidate(servant, "destroy");
            events.removeAll(servant.id());
        }
        for (var task : tasks.list(workflowId)) {
            task.notifyExecutionCompleted(1, "End of test");
        }

        context.close();
    }

    @Test(timeout = 1000)
    public void testAwaitState() throws Exception {
        var s = servantDao.create(userId, workflowName, new Operation.Requirements("s", "a"));
        var t = new Thread(() -> {
            try {
                awaitState(s.workflowName(), s.id(), ServantState.Status.DESTROYED);
            } catch (InterruptedException | DaoException e) {
                LOG.error(e);
                throw new RuntimeException("e");
            }
        });
        t.start();
        servantDao.invalidate(s, "Lol");
        LOG.info("Updated");
        t.join();
    }

    @Test
    public void testSimple() throws Exception {
        var config = new ProcessorConfigBuilder()
            .setIdleTimeout(100)
            .build();
        try (var processor = new ProcessorContext(config)) {
            final CompletableFuture<AllocationRequest> allocationRequested = new CompletableFuture<>();
            allocator.onAllocationRequested(
                ((a, b, c) -> allocationRequested.complete(new AllocationRequest(a, b, c))));
            final var task = processor.generateTask();
            processor.getServant().setTask(task);
            allocationRequested.get();
            processor.getServant().notifyConnected(processor.generateServant());
            processor.env.await();
            processor.servant.notifyConfigured(0, "Ok");
            processor.exec.await();
            processor.servant.notifyExecutionCompleted(0, "Ok");
            awaitState(processor.servant.workflowName(), processor.servant.id(), ServantState.Status.RUNNING);

            final var newTask = tasks.get(task.taskId());
            Assert.assertNotNull(newTask);
            Assert.assertEquals("Ok", newTask.errorDescription());
            Assert.assertEquals(0, Objects.requireNonNull(newTask.rc()).intValue());
            final var servant = servantDao.get(processor.servant.workflowName(), processor.servant.id());
            Assert.assertNull(Objects.requireNonNull(servant).taskId());

            processor.servant.notifyCommunicationCompleted();
            awaitState(processor.servant.workflowName(), processor.servant.id(), ServantState.Status.IDLE);
            processor.stop.await(); // Idle timeout
            processor.servant.notifyStopped(0, "Ok");
            awaitState(processor.servant.workflowName(),
                processor.servant.id(), ServantState.Status.DESTROYED);  //  Destroyed after stop
        }
    }

    @Test
    public void testAllocationTimeout() throws Exception {
        var config = new ProcessorConfigBuilder()
            .setAllocationTimeout(100)
            .build();
        try (var processor = new ProcessorContext(config)) {
            final var task = processor.generateTask();
            processor.getServant().setTask(task);
            awaitState(processor.servant.workflowName(),
                processor.servant.id(), ServantState.Status.DESTROYED);
        }
    }

    @Test
    public void testIdleTimeout() throws Exception {
        var config = new ProcessorConfigBuilder()
            .setIdleTimeout(100)
            .setServantStopTimeout(100)
            .build();
        try (var processor = new ProcessorContext(config)) {
            final CompletableFuture<AllocationRequest> allocationRequested = new CompletableFuture<>();
            allocator.onAllocationRequested(
                ((a, b, c) -> allocationRequested.complete(new AllocationRequest(a, b, c))));
            final var task = processor.generateTask();
            processor.getServant().setTask(task);
            allocationRequested.get();
            processor.getServant().notifyConnected(processor.generateServant());
            processor.env.await();
            processor.servant.notifyConfigured(0, "Ok");
            processor.exec.await();
            processor.servant.notifyExecutionCompleted(0, "Ok");
            awaitState(processor.servant.workflowName(),
                processor.servant.id(), ServantState.Status.DESTROYED);
        }
    }

    @Test
    public void testConfigurationTimeout() throws Exception {
        var config = new ProcessorConfigBuilder()
            .setIdleTimeout(100)
            .setServantStopTimeout(100)
            .setConfiguringTimeout(100)
            .build();
        try (var processor = new ProcessorContext(config)) {
            final CompletableFuture<AllocationRequest> allocationRequested = new CompletableFuture<>();
            allocator.onAllocationRequested(
                ((a, b, c) -> allocationRequested.complete(new AllocationRequest(a, b, c))));
            final var task = processor.generateTask();
            processor.getServant().setTask(task);
            allocationRequested.get();
            processor.getServant().notifyConnected(processor.generateServant());
            awaitState(processor.servant.workflowName(),
                processor.servant.id(), ServantState.Status.DESTROYED);
        }
    }

    @Test
    public void testStoppingTimeout() throws Exception {
        var config = new ProcessorConfigBuilder()
            .setIdleTimeout(100)
            .setServantStopTimeout(100)
            .build();
        try (var processor = new ProcessorContext(config)) {
            final CompletableFuture<AllocationRequest> allocationRequested = new CompletableFuture<>();
            allocator.onAllocationRequested(
                ((a, b, c) -> allocationRequested.complete(new AllocationRequest(a, b, c))));
            final var task = processor.generateTask();
            processor.getServant().setTask(task);
            allocationRequested.get();
            processor.getServant().notifyConnected(processor.generateServant());
            processor.env.await();
            processor.servant.notifyConfigured(0, "Ok");
            processor.servant.stop("Test");
            awaitState(processor.servant.workflowName(),
                processor.servant.id(), ServantState.Status.DESTROYED);
        }
    }

    @Test
    public void testExecutingHeartbeats() throws Exception {
        var config = new ProcessorConfigBuilder()
            .setExecutingHeartbeatPeriod(100)
            .build();
        try (var processor = new ProcessorContext(config)) {
            final CompletableFuture<AllocationRequest> allocationRequested = new CompletableFuture<>();
            allocator.onAllocationRequested(
                ((a, b, c) -> allocationRequested.complete(new AllocationRequest(a, b, c))));
            final var task = processor.generateTask();
            processor.getServant().setTask(task);
            allocationRequested.get();
            processor.getServant().notifyConnected(processor.generateServant());
            processor.env.await();
            processor.servant.notifyConfigured(0, "Ok");
            processor.servant.executingHeartbeat();

            Thread.sleep(50);
            processor.servant.executingHeartbeat();
            Thread.sleep(50);
            processor.servant.executingHeartbeat();
            Thread.sleep(50);
            processor.servant.executingHeartbeat();
            Thread.sleep(50);
            processor.servant.executingHeartbeat();
            awaitState(processor.servant.workflowName(),
                processor.servant.id(), ServantState.Status.DESTROYED);
        }
    }

    @Test
    public void testIdleHeartbeats() throws Exception {
        var config = new ProcessorConfigBuilder()
            .setIdleHeartbeatPeriod(100)
            .build();
        try (var processor = new ProcessorContext(config)) {
            final CompletableFuture<AllocationRequest> allocationRequested = new CompletableFuture<>();
            allocator.onAllocationRequested(
                ((a, b, c) -> allocationRequested.complete(new AllocationRequest(a, b, c))));
            final var task = processor.generateTask();
            processor.getServant().setTask(task);
            allocationRequested.get();
            processor.getServant().notifyConnected(processor.generateServant());
            processor.env.await();
            processor.servant.notifyConfigured(0, "Ok");
            processor.exec.await();
            processor.servant.notifyExecutionCompleted(0, "Ok");
            Thread.sleep(50);
            processor.servant.idleHeartbeat();
            Thread.sleep(50);
            processor.servant.idleHeartbeat();
            Thread.sleep(50);
            processor.servant.idleHeartbeat();
            Thread.sleep(50);
            processor.servant.idleHeartbeat();
            awaitState(processor.servant.workflowName(),
                processor.servant.id(), ServantState.Status.DESTROYED);
        }
    }

    @Test
    public void testFailEnv() throws Exception {
        var config = new ProcessorConfigBuilder()
            .setIdleTimeout(100)
            .setServantStopTimeout(100)
            .build();
        try (var processor = new ProcessorContext(config)) {
            final CompletableFuture<AllocationRequest> allocationRequested = new CompletableFuture<>();
            allocator.onAllocationRequested(
                ((a, b, c) -> allocationRequested.complete(new AllocationRequest(a, b, c))));
            final var task = processor.generateTask();
            processor.getServant().setTask(task);
            allocationRequested.get();
            final var servantURI = processor.generateServant(/*failEnv*/ true, false, false);
            processor.getServant().notifyConnected(servantURI);
            processor.env.await();
            awaitState(processor.servant.workflowName(),
                processor.servant.id(), ServantState.Status.DESTROYED);
        }
    }

    @Test
    public void testFailExec() throws Exception {
        var config = new ProcessorConfigBuilder()
            .setIdleTimeout(100)
            .setServantStopTimeout(100)
            .build();
        try (var processor = new ProcessorContext(config)) {
            final CompletableFuture<AllocationRequest> allocationRequested = new CompletableFuture<>();
            allocator.onAllocationRequested(
                ((a, b, c) -> allocationRequested.complete(new AllocationRequest(a, b, c))));
            final var task = processor.generateTask();
            processor.getServant().setTask(task);
            allocationRequested.get();
            final var servantURI = processor.generateServant(false, /*failExec*/ true, false);
            processor.getServant().notifyConnected(servantURI);
            processor.env.await();
            processor.servant.notifyConfigured(0, "Ok");
            processor.exec.await();
            awaitState(processor.servant.workflowName(),
                processor.servant.id(), ServantState.Status.DESTROYED);  //  Destroyed by timeout
        }
    }

    @Test
    public void testFailStop() throws Exception {
        var config = new ProcessorConfigBuilder()
            .setIdleTimeout(100)
            .setServantStopTimeout(100)
            .build();
        try (var processor = new ProcessorContext(config)) {
            final CompletableFuture<AllocationRequest> allocationRequested = new CompletableFuture<>();
            allocator.onAllocationRequested(
                ((a, b, c) -> allocationRequested.complete(new AllocationRequest(a, b, c))));
            final var task = processor.generateTask();
            processor.getServant().setTask(task);
            allocationRequested.get();
            final var servantURI = processor.generateServant(false, false, /*failStop*/ true);
            processor.servant.notifyConnected(servantURI);
            processor.env.await();
            processor.servant.notifyConfigured(0, "Ok");
            processor.exec.await();
            processor.servant.notifyExecutionCompleted(0, "Ok");
            awaitState(processor.servant.workflowName(), processor.servant.id(), ServantState.Status.RUNNING);

            processor.servant.notifyCommunicationCompleted();
            awaitState(processor.servant.workflowName(), processor.servant.id(), ServantState.Status.IDLE);
            awaitState(processor.servant.workflowName(),
                processor.servant.id(), ServantState.Status.DESTROYED);  //  Destroyed by timeout
        }
    }

    @Test
    public void testRestore() throws Exception {
        var config = new ProcessorConfigBuilder()
            .setIdleTimeout(100)
            .build();
        String servantId;
        final CompletableFuture<AllocationRequest> allocationRequested;

        try (var processor = new ProcessorContext(config)) {
            servantId = processor.servant.id();
            allocationRequested = new CompletableFuture<>();
            allocator.onAllocationRequested(
                ((a, b, c) -> allocationRequested.complete(new AllocationRequest(a, b, c))));
            final var task = processor.generateTask();
            processor.getServant().setTask(task);
        }

        final var port = FreePortFinder.find(1000, 2000);
        final CountDownLatch env = new CountDownLatch(1);
        final CountDownLatch exec = new CountDownLatch(1);
        final CountDownLatch stop = new CountDownLatch(1);

        final var mock = new AllocatedServantMock.ServantBuilder(port)
            .setOnEnv(env::countDown)
            .setOnExec(exec::countDown)
            .setOnStop(stop::countDown)
            .build();

        try (var processor = new ProcessorContext(servantId, config)) {
            allocationRequested.get();
            awaitState(processor.servant.workflowName(), processor.servant.id(),
                ServantState.Status.CONNECTING);
            processor.servant.notifyConnected(HostAndPort.fromParts("localhost", port));
        }

        try (var processor = new ProcessorContext(servantId, config)) {
            env.await();
            processor.servant.notifyConfigured(0, "Ok");
        }
        try (var processor = new ProcessorContext(servantId, config)) {
            exec.await();
            processor.servant.notifyExecutionCompleted(0, "Ok");
        }
        try (var processor = new ProcessorContext(servantId, config)) {
            awaitState(processor.servant.workflowName(), processor.servant.id(), ServantState.Status.RUNNING);
            processor.servant.notifyCommunicationCompleted();
        }
        try (var processor = new ProcessorContext(servantId, config)) {
            awaitState(processor.servant.workflowName(), processor.servant.id(), ServantState.Status.IDLE);
            awaitState(processor.servant.workflowName(),
                processor.servant.id(), ServantState.Status.STOPPING);  // Idle timeout
            processor.servant.notifyStopped(0, "Ok");
        }
        try (var processor = new ProcessorContext(servantId, config)) {
            awaitState(processor.servant.workflowName(),
                processor.servant.id(), ServantState.Status.DESTROYED);  //  Destroyed after stop
        }

        mock.close();
    }

    record AllocationRequest(String workflowId, String servantId, String token) {

    }

    public static Operation buildOp(String... tags) {
        return new Operation(null, new Operation.Requirements("", ""), "", List.of(), "", "", null, null);
    }

    public class ProcessorContext implements AutoCloseable {

        private final Servant servant;
        private final ServantEventProcessor processor;
        private final CountDownLatch latch = new CountDownLatch(1);
        private final String[] tags;
        private final CountDownLatch env = new CountDownLatch(1);
        private final CountDownLatch exec = new CountDownLatch(1);
        private final CountDownLatch stop = new CountDownLatch(1);
        private AllocatedServantMock mock;

        public ProcessorContext(ServantEventProcessorConfig config, String... provisioningTags) throws DaoException {
            servant = servantDao.create(userId, workflowName, new Operation.Requirements("s", "a"));
            processor = new ServantEventProcessor(workflowName, servant.id(), config, allocator, tasks, events,
                servantDao, manager, (a, b) -> latch.countDown(), (a, b) -> {
            });
            processor.start();
            tags = provisioningTags;
        }

        public ProcessorContext(String servantId,
            ServantEventProcessorConfig config, String... provisioningTags) throws DaoException {
            servant = Objects.requireNonNull(servantDao.get(workflowName, servantId));
            processor = new ServantEventProcessor(workflowName, servantId, config, allocator, tasks, events,
                servantDao, manager, (a, b) -> latch.countDown(), (a, b) -> {
            });
            processor.start();
            tags = provisioningTags;
        }

        public Task generateTask() throws DaoException {
            return tasks.create(workflowId, workflowName, userId, new TaskDesc(buildOp(tags), Map.of()));
        }

        public HostAndPort generateServant() throws IOException {
            return generateServant(false, false, false);
        }

        public HostAndPort generateServant(boolean failEnv, boolean failExec, boolean failStop) throws IOException {
            final var port = FreePortFinder.find(10000, 20000);
            mock = new AllocatedServantMock.ServantBuilder(port)
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
            return HostAndPort.fromParts("localhost", port);
        }

        public Servant getServant() {
            return servant;
        }

        @Override
        public void close() throws InterruptedException {
            processor.shutdown();
            processor.join();
            if (mock != null) {
                mock.close();
            }
        }
    }

    public void awaitState(String workflowName, String servantId,
        ServantState.Status status) throws InterruptedException, DaoException {
        ServantState.Status s = null;
        var servant = servantDao.get(workflowName, servantId);
        if (servant != null) {
            s = servant.status();
        }
        while (s == null || s != status) {
            LOG.debug("Got status {}, awaiting {}", s, status);
            Thread.sleep(10);
            servant = servantDao.get(workflowName, servantId);
            if (servant == null) {
                s = null;
            } else {
                s = servant.status();
            }
        }
    }
}
