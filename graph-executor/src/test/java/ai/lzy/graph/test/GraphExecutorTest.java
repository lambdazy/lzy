package ai.lzy.graph.test;

import ai.lzy.graph.config.ServiceConfig;
import ai.lzy.graph.exec.BfsGraphProcessor;
import ai.lzy.graph.exec.ChannelCheckerFactory;
import ai.lzy.graph.exec.GraphProcessor;
import ai.lzy.graph.model.ChannelDescription;
import ai.lzy.graph.model.GraphDescription;
import ai.lzy.graph.model.GraphExecutionState;
import ai.lzy.graph.model.TaskDescription;
import ai.lzy.graph.queue.QueueManager;
import ai.lzy.graph.test.mocks.GraphDaoMock;
import ai.lzy.graph.test.mocks.SchedulerApiMock;
import ai.lzy.priv.v2.SchedulerApi.TaskStatus;
import io.grpc.StatusException;
import io.micronaut.context.ApplicationContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import ai.lzy.model.db.DaoException;
import ai.lzy.graph.db.QueueEventDao;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.*;
import ai.lzy.graph.algo.GraphBuilderImpl;
import ai.lzy.graph.algo.GraphBuilder;
import ai.lzy.model.Slot;
import ai.lzy.model.Zygote;
import ai.lzy.model.data.DataSchema;
import org.junit.rules.Timeout;

import java.util.stream.Collectors;

import static ai.lzy.priv.v2.SchedulerApi.TaskStatus.StatusCase.ERROR;
import static ai.lzy.priv.v2.SchedulerApi.TaskStatus.StatusCase.QUEUE;

@MicronautTest
public class GraphExecutorTest {

    private SchedulerApiMock scheduler;
    private static final ApplicationContext context = ApplicationContext.run();
    private static final GraphDaoMock dao = context.getBean(GraphDaoMock.class);
    private static final QueueEventDao queueEventDao = context.getBean(QueueEventDao.class);

    @Rule
    public Timeout globalTimeout = Timeout.seconds(10);

    @Before
    public void setUp() {
        scheduler = new SchedulerApiMock((a, b, sch) -> {
            sch.changeStatus(b.id(), TaskStatus.newBuilder()
                .setTaskId(b.id())
                .setQueue(TaskStatus.Queue.newBuilder().build())
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
        for (var graph: graphs) {
            dao.free(graph.copyFromThis()
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
    }

    @Test
    public void testSimple() throws InterruptedException, StatusException, DaoException {
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
            tester.waitForStatus(QUEUE, "1", "3", "5", "7", "9");
            tester.awaitExecutingNow("1", "3", "5", "7", "9");
            tester.changeStatus(SchedulerApiMock.COMPLETED, "1", "5", "7", "9");

            // Step 2
            tester.waitForStatus(QUEUE, "3", "6", "10");
            tester.awaitExecutingNow("3", "10");
            tester.changeStatus(SchedulerApiMock.COMPLETED, "6", "10");

            // Step 3
            tester.waitForStatus(QUEUE, "3", "8");
            tester.awaitExecutingNow("3");
            tester.changeStatus(SchedulerApiMock.COMPLETED, "3", "8");

            // Step 4
            tester.waitForStatus(QUEUE, "2", "4");
            tester.awaitExecutingNow();
            tester.changeStatus(SchedulerApiMock.COMPLETED, "2", "4");

            // Step 5
            tester.waitForStatus(GraphExecutionState.Status.COMPLETED);

        }
    }

    @Test
    public void testErrorInTask() throws InterruptedException, StatusException, DaoException {

        final GraphDescription graph = new GraphDescriptionBuilder()
            .addVertexes("1", "2", "3")
            .addEdge("1", "2")
            .addEdge("1", "3")
            .build();

        try (var tester = new GraphTester(graph)) {
            // Step 1
            tester.waitForStatus(QUEUE, "1");
            tester.awaitExecutingNow("1");
            tester.changeStatus(SchedulerApiMock.COMPLETED, "1");

            // Step 2
            tester.waitForStatus(QUEUE, "2");
            tester.changeStatus(SchedulerApiMock.EXECUTING, "2", "3");

            // Step 3
            tester.changeStatus(SchedulerApiMock.ERROR, "1");
            tester.waitForStatus(GraphExecutionState.Status.FAILED);
            tester.waitForStatus(ERROR, "1", "2", "3");
        }
    }

    @Test
    public void testStop() throws InterruptedException, StatusException, DaoException {
        final GraphDescription graph = new GraphDescriptionBuilder()
            .addVertexes("1", "2", "3")
            .addEdge("1", "2")
            .addEdge("1", "3")
            .build();

        try (var tester = new GraphTester(graph)) {
            // Step 1
            tester.waitForStatus(QUEUE, "1");
            tester.awaitExecutingNow("1");
            tester.changeStatus(SchedulerApiMock.COMPLETED, "1");

            // Step 2
            tester.waitForStatus(QUEUE, "2", "3");
            tester.changeStatus(SchedulerApiMock.EXECUTING, "2", "3");

            tester.queue.stopGraph("", tester.state.id(), "Stopped from test");

            //Step 3
            tester.waitForStatus(ERROR, "1", "2", "3");
            tester.waitForStatus(GraphExecutionState.Status.FAILED);
        }
    }

    @Test
    public void testCycles() throws InterruptedException, StatusException, DaoException {
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
            tester.waitForStatus(QUEUE, "1", "2");
            tester.awaitExecutingNow("1", "2");
            tester.changeStatus(SchedulerApiMock.COMPLETED, "1");

            // Step2
            tester.awaitExecutingNow("1", "2");
            tester.changeStatus(SchedulerApiMock.COMPLETED, "2");

            // Step 3
            tester.waitForStatus(QUEUE, "3", "4", "5");
            tester.awaitExecutingNow();
            tester.changeStatus(SchedulerApiMock.COMPLETED, "3", "4", "5");

            //Step 4
            tester.waitForStatus(GraphExecutionState.Status.COMPLETED);
        }
    }

    @Test
    public void testRestore() throws InterruptedException, DaoException, StatusException {
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

        final String workflowId, graphId;

        try (var tester = new GraphTester(graph)) {
            workflowId = tester.state.workflowId();
            graphId = tester.state.id();

            tester.waitForStatus(QUEUE, "1", "3", "5", "7", "9");
            tester.awaitExecutingNow("1", "3", "5", "7", "9");
            tester.changeStatus(SchedulerApiMock.COMPLETED, "1", "5", "7", "9");

            // Step 2
            tester.waitForStatus(QUEUE, "3", "6", "10");
            tester.awaitExecutingNow("3", "10");
            tester.changeStatus(SchedulerApiMock.COMPLETED, "6", "10");

            // Step 3
            tester.waitForStatus(QUEUE, "3", "8");
            tester.awaitExecutingNow("3");
            tester.changeStatus(SchedulerApiMock.COMPLETED, "3", "8");
        }

        try (var tester = new GraphTester(workflowId, graphId)) {

            // Step 4
            tester.waitForStatus(QUEUE, "2", "4");
            tester.awaitExecutingNow();
            tester.changeStatus(SchedulerApiMock.COMPLETED, "2", "4");

            // Step 5
            tester.waitForStatus(GraphExecutionState.Status.COMPLETED);

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

        GraphTester(GraphDescription graph) throws StatusException {
            this.graph = graph;
            this.queue = initQueue();
            this.queue.start();
            state = this.queue.startGraph("", "changeMe", graph);
        }

        public void awaitExecutingNow(String... taskIds) throws InterruptedException, DaoException {
            dao.waitForExecutingNow("", state.id(), new HashSet<>(Arrays.stream(taskIds).toList()));
        }

        public void changeStatus(TaskStatus s, String... taskIds) {
            for (String task : taskIds) {
                scheduler.changeStatus(task, s);
            }
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

    public static Zygote buildZygote(List<String> inputs, List<String> outputs) {
        return new Zygote() {
            @Override
            public Slot[] input() {
                return inputs.stream().map(t -> buildSlot(t, Slot.Direction.INPUT)).toArray(Slot[]::new);
            }

            @Override
            public Slot[] output() {
                return outputs.stream().map(t -> buildSlot(t, Slot.Direction.OUTPUT)).toArray(Slot[]::new);
            }

            @Override
            public void run() {
            }

            @Override
            public String name() {
                return null;
            }
        };
    }

    public static Slot buildSlot(String name, Slot.Direction direction) {
        return new Slot() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public Media media() {
                return Media.FILE;
            }

            @Override
            public Direction direction() {
                return direction;
            }

            @Override
            public DataSchema contentType() {
                return DataSchema.plain;
            }
        };
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
                Zygote zygote = buildZygote(inputs, outputs);
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
        ServiceConfig config = new ServiceConfig(1234, 1, new ServiceConfig.Scheduler("localhost", 1));
        return new QueueManager(processor, dao, config, queueEventDao);
    }
}
