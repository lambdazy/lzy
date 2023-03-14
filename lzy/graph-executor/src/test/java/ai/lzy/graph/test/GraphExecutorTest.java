package ai.lzy.graph.test;

import ai.lzy.graph.algo.GraphBuilder;
import ai.lzy.graph.algo.GraphBuilderImpl;
import ai.lzy.graph.config.ServiceConfig;
import ai.lzy.graph.db.QueueEventDao;
import ai.lzy.graph.db.impl.GraphExecutorDataSource;
import ai.lzy.graph.exec.BfsGraphProcessor;
import ai.lzy.graph.exec.ChannelCheckerFactory;
import ai.lzy.graph.exec.GraphProcessor;
import ai.lzy.graph.model.ChannelDescription;
import ai.lzy.graph.model.GraphDescription;
import ai.lzy.graph.model.GraphExecutionState;
import ai.lzy.graph.model.TaskDescription;
import ai.lzy.graph.queue.QueueManager;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.exceptions.DaoException;
import ai.lzy.v1.common.LMD;
import ai.lzy.v1.common.LMO;
import ai.lzy.v1.common.LMS;
import ai.lzy.v1.scheduler.Scheduler.TaskStatus;
import io.micronaut.context.ApplicationContext;
import io.micronaut.inject.qualifiers.Qualifiers;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import org.junit.*;
import org.junit.rules.Timeout;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ai.lzy.model.db.test.DatabaseTestUtils.preparePostgresConfig;
import static ai.lzy.v1.common.LMS.Slot.Direction.INPUT;
import static ai.lzy.v1.common.LMS.Slot.Direction.OUTPUT;
import static ai.lzy.v1.scheduler.Scheduler.TaskStatus.StatusCase.ERROR;
import static ai.lzy.v1.scheduler.Scheduler.TaskStatus.StatusCase.EXECUTING;

public class GraphExecutorTest {

    @Rule
    public Timeout globalTimeout = Timeout.seconds(10);
    @Rule
    public PreparedDbRule db = EmbeddedPostgresRules.preparedDatabase(ds -> {});

    private ApplicationContext context;
    private GraphDaoMock dao;
    private QueueEventDao queueEventDao;
    private OperationDao operationDao;
    private SchedulerApiMock scheduler;
    private GraphExecutorDataSource storage;

    @Before
    public void setUp() {
        context = ApplicationContext.run(preparePostgresConfig("graph-executor", db.getConnectionInfo()), "test-mock");
        dao = context.getBean(GraphDaoMock.class);
        queueEventDao = context.getBean(QueueEventDao.class);
        storage = context.getBean(GraphExecutorDataSource.class);
        operationDao = context.getBean(OperationDao.class, Qualifiers.byName("GraphExecutorOperationDao"));

        scheduler = new SchedulerApiMock((a, b, sch) -> {
            sch.changeStatus(b.id(), TaskStatus.newBuilder()
                .setTaskId(b.id())
                .setExecuting(TaskStatus.Executing.newBuilder().build())
                .build()
            );
            return b.id();
        });
    }

    @After
    public void tearDown() throws DaoException {
        scheduler = null;
        var graphs = dao.filter(GraphExecutionState.Status.EXECUTING);
        graphs.addAll(dao.filter(GraphExecutionState.Status.WAITING));
        for (var graph : graphs) {
            dao.updateAndFree(graph.copyFromThis()
                .withStatus(GraphExecutionState.Status.FAILED)
                .build());
        }
        queueEventDao.removeAllAcquired();
        var events = queueEventDao.acquireWithLimit(100);
        while (events.size() > 0) {
            for (var event : events) {
                queueEventDao.remove(event);
            }
            events = queueEventDao.acquireWithLimit(100);
        }

        context.close();
    }

    @Test
    public void testSimple() throws Exception {
        final GraphDescription graph = new GraphDescriptionBuilder()
            .addVertexes("1", "2", "3", "4", "5", "6", "7", "8", "9", "10")
            .addEdge("1", "2")
            .addEdge("3", "2")
            .addEdge("3", "4")
            .addEdge("5", "6")
            .addEdge("7", "6")
            .addEdge("7", "8")
            .addEdge("9", "10")
            .addEdge("10", "8")
            .build();

        try (var tester = new GraphTester(graph)) {

            // Step 1
            tester.waitForStatus(EXECUTING, "1", "3", "5", "7", "9");
            tester.awaitExecutingNow("1", "3", "5", "7", "9");
            tester.changeStatus(SchedulerApiMock.COMPLETED, "1", "5", "7", "9");

            // Step 2
            tester.waitForStatus(EXECUTING, "3", "6", "10");
            tester.awaitExecutingNow("3", "10", "6");
            tester.changeStatus(SchedulerApiMock.COMPLETED, "6", "10");

            // Step 3
            tester.waitForStatus(EXECUTING, "3", "8");
            tester.awaitExecutingNow("3", "8");
            tester.changeStatus(SchedulerApiMock.COMPLETED, "3", "8");

            // Step 4
            tester.waitForStatus(EXECUTING, "2", "4");
            tester.awaitExecutingNow("2", "4");
            tester.changeStatus(SchedulerApiMock.COMPLETED, "2", "4");

            // Step 5
            tester.waitForStatus(GraphExecutionState.Status.COMPLETED);

        }
    }

    @Test
    public void testErrorInTask() throws Exception {

        final GraphDescription graph = new GraphDescriptionBuilder()
            .addVertexes("1", "2", "3")
            .addEdge("1", "2")
            .addEdge("1", "3")
            .build();

        try (var tester = new GraphTester(graph)) {
            // Step 1
            tester.waitForStatus(EXECUTING, "1");
            tester.awaitExecutingNow("1");
            tester.changeStatus(SchedulerApiMock.COMPLETED, "1");

            // Step 2
            tester.waitForStatus(EXECUTING, "2", "3");
            tester.changeStatus(SchedulerApiMock.EXECUTING, "2", "3");

            // Step 3
            tester.changeStatus(SchedulerApiMock.ERROR, "1");
            tester.waitForStatus(GraphExecutionState.Status.FAILED);
            tester.waitForStatus(ERROR, "1", "2", "3");
        }
    }

    @Test
    public void testStop() throws Exception {
        final GraphDescription graph = new GraphDescriptionBuilder()
            .addVertexes("1", "2", "3")
            .addEdge("1", "2")
            .addEdge("1", "3")
            .build();

        try (var tester = new GraphTester(graph)) {
            // Step 1
            tester.waitForStatus(EXECUTING, "1");
            tester.awaitExecutingNow("1");
            tester.changeStatus(SchedulerApiMock.COMPLETED, "1");

            // Step 2
            tester.waitForStatus(EXECUTING, "2", "3");
            tester.changeStatus(SchedulerApiMock.EXECUTING, "2", "3");

            tester.queue.stopGraph("", tester.state.id(), "Stopped from test");

            //Step 3
            tester.waitForStatus(ERROR, "1", "2", "3");
            tester.waitForStatus(GraphExecutionState.Status.FAILED);
        }
    }

    @Test
    public void testCycles() throws Exception {
        final GraphDescription graph = new GraphDescriptionBuilder()
            .addVertexes("1", "2", "3", "4", "5")
            .addEdge("1", "2")
            .addEdge("2", "1")
            .addEdge("1", "3")
            .addEdge("3", "4")
            .addEdge("4", "5")
            .addEdge("5", "3")
            .build();

        try (var tester = new GraphTester(graph)) {
            // Step 1
            tester.waitForStatus(EXECUTING, "1", "2");
            tester.awaitExecutingNow("1", "2");
            tester.changeStatus(SchedulerApiMock.COMPLETED, "2");

            // Step2
            tester.awaitExecutingNow("1", "2");
            tester.changeStatus(SchedulerApiMock.COMPLETED, "1");

            // Step 3
            tester.waitForStatus(EXECUTING, "3", "4", "5");
            tester.awaitExecutingNow("3", "4", "5");
            tester.changeStatus(SchedulerApiMock.COMPLETED, "3", "4", "5");

            //Step 4
            tester.waitForStatus(GraphExecutionState.Status.COMPLETED);
        }
    }

    @Test
    public void testRestore() throws Exception {
        final GraphDescription graph = new GraphDescriptionBuilder()
            .addVertexes("1", "2", "3", "4", "5", "6", "7", "8", "9", "10")
            .addEdge("1", "2")
            .addEdge("3", "2")
            .addEdge("3", "4")
            .addEdge("5", "6")
            .addEdge("7", "6")
            .addEdge("7", "8")
            .addEdge("9", "10")
            .addEdge("10", "8")
            .build();

        final String workflowId;
        final String graphId;

        try (var tester = new GraphTester(graph)) {
            workflowId = tester.state.workflowId();
            graphId = tester.state.id();

            tester.waitForStatus(EXECUTING, "1", "3", "5", "7", "9");
            tester.awaitExecutingNow("1", "3", "5", "7", "9");
            tester.changeStatus(SchedulerApiMock.COMPLETED, "1", "5", "7", "9");

            // Step 2
            tester.waitForStatus(EXECUTING, "3", "6", "10");
            tester.awaitExecutingNow("3", "6", "10");
            tester.changeStatus(SchedulerApiMock.COMPLETED, "6", "10");

            // Step 3
            tester.waitForStatus(EXECUTING, "3", "8");
            tester.awaitExecutingNow("3", "8");
            tester.changeStatus(SchedulerApiMock.COMPLETED, "3", "8");
        }

        try (var tester = new GraphTester(workflowId, graphId)) {

            // Step 4
            tester.waitForStatus(EXECUTING, "2", "4");
            tester.awaitExecutingNow("2", "4");
            tester.changeStatus(SchedulerApiMock.COMPLETED, "2", "4");

            // Step 5
            tester.waitForStatus(GraphExecutionState.Status.COMPLETED);

        }
    }

    @Test
    public void testOneVertex() throws Exception {
        final GraphDescription graph = new GraphDescriptionBuilder()
            .addVertexes("1")
            .build();

        try (var tester = new GraphTester(graph)) {

            // Step 1
            tester.waitForStatus(EXECUTING, "1");
            tester.awaitExecutingNow("1");
            tester.changeStatus(SchedulerApiMock.COMPLETED, "1");

            // Step 2
            tester.waitForStatus(GraphExecutionState.Status.COMPLETED);

        }
    }

    @Test
    public void testRhombus() throws Exception {
        final GraphDescription graph = new GraphDescriptionBuilder()
            .addVertexes("1", "2", "3", "4")
            .addEdge("1", "2")
            .addEdge("1", "3")
            .addEdge("2", "4")
            .addEdge("3", "4")
            .build();

        try (var tester = new GraphTester(graph)) {

            // Step 1
            tester.waitForStatus(EXECUTING, "1");
            tester.awaitExecutingNow("1");
            tester.changeStatus(SchedulerApiMock.COMPLETED, "1");

            // Step 2
            tester.waitForStatus(EXECUTING, "2", "3");
            tester.awaitExecutingNow("2", "3");
            tester.changeStatus(SchedulerApiMock.COMPLETED, "3");

            // Step 3
            tester.waitForStatus(EXECUTING, "2");
            tester.awaitExecutingNow("2");
            tester.changeStatus(SchedulerApiMock.COMPLETED, "2");

            // Step 4
            tester.waitForStatus(EXECUTING, "4");
            tester.changeStatus(SchedulerApiMock.COMPLETED, "4");

            // Step 2
            tester.waitForStatus(GraphExecutionState.Status.COMPLETED);

        }
    }

    @Test
    public void testError() throws Exception {
        final GraphDescription graph = new GraphDescriptionBuilder()
            .addVertexes("1")
            .build();

        try (var tester = new GraphTester(graph)) {

            // Step 1
            tester.waitForStatus(EXECUTING, "1");
            tester.awaitExecutingNow("1");
            tester.raiseSchedulerException("1");

            // Step 2
            tester.waitForStatus(GraphExecutionState.Status.FAILED);

        }
    }

    private class GraphTester implements AutoCloseable {

        private final GraphDescription graph;
        private final GraphExecutionState state;
        private final QueueManager queue;

        GraphTester(String workflowId, String graphId) throws DaoException {
            this.state = dao.get(workflowId, graphId);
            Assert.assertNotNull(state);
            graph = state.description();
            this.queue = initQueue();
            this.queue.start();
        }

        GraphTester(GraphDescription graph) throws Exception {
            this.graph = graph;
            this.queue = initQueue();
            this.queue.start();
            state = this.queue.startGraph("", "changeMe", "uid", graph, null);
        }

        public void awaitExecutingNow(String... taskIds) throws InterruptedException, DaoException {
            dao.waitForExecutingNow("", state.id(), new HashSet<>(Arrays.stream(taskIds).toList()));
        }

        public void changeStatus(TaskStatus s, String... taskIds) {
            for (String task : taskIds) {
                scheduler.changeStatus(task, s);
            }
        }

        public void raiseSchedulerException(String taskId) {
            scheduler.raiseException(taskId);
        }

        public void waitForStatus(TaskStatus.StatusCase s, String... taskIds) throws InterruptedException {
            for (String task : taskIds) {
                scheduler.waitForStatus(task, s);
            }
        }

        public void waitForStatus(GraphExecutionState.Status s) throws InterruptedException, DaoException {
            dao.waitForStatus(state.workflowId(), state.id(), s);
            Assert.assertEquals(s, dao.get(state.workflowId(), state.id()).status());
        }

        @Override
        public void close() throws InterruptedException {
            queue.gracefulShutdown();
            queue.join();
        }
    }

    public static LMO.Operation buildOperation(List<String> inputs, List<String> outputs) {
        final var slots = Stream.concat(
                inputs.stream()
                    .map(s -> buildSlot(s, INPUT)),
                outputs.stream()
                    .map(s -> buildSlot(s, OUTPUT)))
            .toList();
        return LMO.Operation.newBuilder()
            .setRequirements(LMO.Requirements.newBuilder()
                .setZone("")
                .setPoolLabel("")
                .build())
            .addAllSlots(slots)
            .build();
    }

    public static LMS.Slot buildSlot(String name, LMS.Slot.Direction direction) {

        return LMS.Slot.newBuilder()
            .setContentType(LMD.DataScheme.newBuilder().build())
            .setName(name)
            .setDirection(direction)
            .build();
    }

    public static class GraphDescriptionBuilder {

        private final List<String> vertexes = new ArrayList<>();
        private final Map<String, List<String>> edges = new HashMap<>();
        private final Map<String, List<String>> reversedEdges = new HashMap<>();

        public GraphDescriptionBuilder addEdge(String input, String output) {
            edges.computeIfAbsent(input, k -> new ArrayList<>()).add(output);
            reversedEdges.computeIfAbsent(output, k -> new ArrayList<>()).add(input);
            return this;
        }

        public GraphDescriptionBuilder addVertexes(String... vertexNames) {
            vertexes.addAll(Arrays.stream(vertexNames).toList());
            return this;
        }

        public GraphDescription build() {
            List<TaskDescription> tasks = new ArrayList<>();
            Map<String, ChannelDescription> channelDescriptions = new HashMap<>();
            for (String v : vertexes) {
                List<String> inputs = reversedEdges.getOrDefault(v, new ArrayList<>()).stream()
                    .map(s -> s + "to" + v).collect(Collectors.toList());
                List<String> outputs = edges.getOrDefault(v, new ArrayList<>()).stream()
                    .map(s -> v + "to" + s).collect(Collectors.toList());
                final var zygote = buildOperation(inputs, outputs);
                Map<String, String> slotsMapping = Stream.concat(inputs.stream(), outputs.stream())
                    .collect(Collectors.toMap(t -> t, t -> t));
                channelDescriptions.putAll(
                    slotsMapping.values()
                        .stream()
                        .map(t -> new ChannelDescription(ChannelDescription.Type.DIRECT, t))
                        .collect(Collectors.toMap(ChannelDescription::id, t -> t))
                );
                tasks.add(new TaskDescription(v, zygote, slotsMapping));
            }
            return new GraphDescription(tasks, channelDescriptions);
        }
    }

    private QueueManager initQueue() {
        GraphBuilder builder = new GraphBuilderImpl();
        ChannelCheckerFactory factory = new ChannelCheckerFactory(scheduler);
        GraphProcessor processor = new BfsGraphProcessor(scheduler, builder, factory);

        ServiceConfig config = new ServiceConfig();
        config.setPort(1234);
        config.setExecutorsCount(1);
        config.setScheduler(new ServiceConfig.Scheduler());

        return new QueueManager(config, processor, storage, queueEventDao, dao, operationDao);
    }
}
