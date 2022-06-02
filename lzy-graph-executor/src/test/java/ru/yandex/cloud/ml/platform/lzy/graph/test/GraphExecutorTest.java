package ru.yandex.cloud.ml.platform.lzy.graph.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import ru.yandex.cloud.ml.platform.lzy.graph.exec.BfsGraphProcessor;
import ru.yandex.cloud.ml.platform.lzy.graph.exec.GraphProcessor;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import ru.yandex.cloud.ml.platform.lzy.graph.exec.GraphExecutor;
import ru.yandex.cloud.ml.platform.lzy.graph.algo.GraphBuilderImpl;
import ru.yandex.cloud.ml.platform.lzy.graph.algo.GraphBuilder;
import ru.yandex.cloud.ml.platform.lzy.graph.model.GraphDescription;
import ru.yandex.cloud.ml.platform.lzy.graph.model.GraphExecutionState;
import ru.yandex.cloud.ml.platform.lzy.graph.model.TaskDescription;
import ru.yandex.cloud.ml.platform.lzy.graph.test.mocks.GraphDaoMock;
import ru.yandex.cloud.ml.platform.lzy.graph.test.mocks.SchedulerApiMock;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.Zygote;
import ru.yandex.cloud.ml.platform.lzy.model.data.DataSchema;
import yandex.cloud.priv.datasphere.v2.lzy.Tasks;

import java.util.Set;
import java.util.stream.Collectors;

@MicronautTest
public class GraphExecutorTest {

    private SchedulerApiMock scheduler;
    private GraphDaoMock dao;
    private GraphExecutor executor;
    private final int TIMEOUT = 100;

    @Before
    public void setUp() {
        scheduler =  new SchedulerApiMock((a, b, sch) -> {
            sch.changeStatus(b.id(), Tasks.TaskProgress.newBuilder()
                .setTid(b.id())
                .setStatus(Tasks.TaskProgress.Status.QUEUE)
                .build()
            );
            return b.id();
        });

        dao = new GraphDaoMock();

        GraphBuilder builder = new GraphBuilderImpl();
        GraphProcessor processor = new BfsGraphProcessor(scheduler, builder);
        executor = new GraphExecutor(dao, processor);

        Configurator.setAllLevels("", Level.DEBUG);
    }

    @Test
    public void testSimple() throws InterruptedException {
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
            tester.waitForStatus(Tasks.TaskProgress.Status.QUEUE, "1", "3", "5", "7", "9");
            tester.assertExecutingNow("1", "3", "5", "7", "9");
            tester.changeStatus(SchedulerApiMock.EXECUTING, "1", "5", "7", "9");

            // Step 2
            tester.waitForStatus(Tasks.TaskProgress.Status.QUEUE, "3", "6", "10");
            tester.assertExecutingNow("3", "6", "10");
            tester.changeStatus(SchedulerApiMock.EXECUTING, "6", "10");

            // Step 3
            tester.waitForStatus(Tasks.TaskProgress.Status.QUEUE, "3", "8");
            tester.assertExecutingNow("3", "8");
            tester.changeStatus(SchedulerApiMock.EXECUTING, "3", "8");

            // Step 4
            tester.waitForStatus(Tasks.TaskProgress.Status.QUEUE, "2", "4");
            tester.assertExecutingNow("2", "4");
            tester.changeStatus(SchedulerApiMock.EXECUTING, "2", "4");

            // Step 5
            tester.assertExecutingNow();
            tester.changeStatus(SchedulerApiMock.COMPLETED, "1", "2", "3", "4", "5", "6", "7", "8", "9", "10");
            tester.waitForStatus(GraphExecutionState.Status.COMPLETED);

        }
    }

    @Test
    public void testErrorInTask() throws InterruptedException {

        final GraphDescription graph = new GraphDescriptionBuilder()
            .addVertexes("1", "2", "3")
            .addEdge("1", "2")
            .addEdge("1", "3")
            .build();

        try (var tester = new GraphTester(graph)) {
            // Step 1
            tester.waitForStatus(Tasks.TaskProgress.Status.QUEUE, "1");
            tester.assertExecutingNow("1");
            tester.changeStatus(SchedulerApiMock.EXECUTING, "1");

            // Step 2
            tester.waitForStatus(Tasks.TaskProgress.Status.QUEUE, "2");
            tester.assertExecutingNow("2", "3");
            tester.changeStatus(SchedulerApiMock.EXECUTING, "2", "3");

            // Step 3
            tester.changeStatus(SchedulerApiMock.ERROR, "1");
            tester.waitForStatus(GraphExecutionState.Status.FAILED);
            tester.waitForStatus(Tasks.TaskProgress.Status.ERROR, "1", "2", "3");
        }
    }

    @Test
    public void testStop() throws InterruptedException {
        final GraphDescription graph = new GraphDescriptionBuilder()
            .addVertexes("1", "2", "3")
            .addEdge("1", "2")
            .addEdge("1", "3")
            .build();

        try (var tester = new GraphTester(graph)) {
            // Step 1
            tester.waitForStatus(Tasks.TaskProgress.Status.QUEUE, "1");
            tester.assertExecutingNow("1");
            tester.changeStatus(SchedulerApiMock.EXECUTING, "1");

            // Step 2
            tester.waitForStatus(Tasks.TaskProgress.Status.QUEUE, "2");
            tester.assertExecutingNow("2", "3");
            tester.changeStatus(SchedulerApiMock.EXECUTING, "2", "3");
            dao.updateAtomic("", tester.state.id(), s -> new GraphExecutionState(
                s.workflowId(),
                s.id(),
                s.description(),
                s.executions(),
                s.currentExecutionGroup(),
                GraphExecutionState.Status.SCHEDULED_TO_FAIL,
                "Stopped by test"
            ));

            //Step 3
            tester.waitForStatus(Tasks.TaskProgress.Status.ERROR, "1", "2", "3");
            tester.waitForStatus(GraphExecutionState.Status.FAILED);
        }
    }

    @Test
    public void testCycles() throws InterruptedException {
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
            tester.waitForStatus(Tasks.TaskProgress.Status.QUEUE, "1", "2");
            tester.assertExecutingNow("1", "2");
            tester.changeStatus(SchedulerApiMock.EXECUTING, "1");
            tester.changeStatus(SchedulerApiMock.QUEUE, "2");

            // Step2
            tester.assertExecutingNow("1", "2");
            tester.changeStatus(SchedulerApiMock.EXECUTING, "2");

            // Step 3
            tester.waitForStatus(Tasks.TaskProgress.Status.QUEUE, "3", "4", "5");
            tester.assertExecutingNow("3", "4", "5");
            tester.changeStatus(SchedulerApiMock.COMPLETED, "1", "2", "3", "4", "5");

            //Step 4
            tester.waitForStatus(GraphExecutionState.Status.COMPLETED);
            tester.assertExecutingNow();
        }
    }

    private class GraphTester implements AutoCloseable {
        private final GraphDescription graph;
        private GraphExecutionState state;

        GraphTester(GraphDescription graph) {
            this.graph = graph;
            state = dao.create("", graph);
            executor.start();
        }

        public void assertExecutingNow(String... taskIds) {
            final GraphExecutionState currentState = dao.get(state.workflowId(), state.id());
            Assert.assertNotNull(currentState);
            Assert.assertEquals(
                Set.of(taskIds),
                currentState.currentExecutionGroup().stream().map(t -> t.description().id()).collect(Collectors.toSet())
            );
        }

        public void changeStatus(Tasks.TaskProgress s, String... taskIds) {
            for (String task: taskIds) {
                scheduler.changeStatus(task, s);
            }
        }

        public void waitForStatus(Tasks.TaskProgress.Status s, String... taskIds) throws InterruptedException {
            for (String task: taskIds) {
                scheduler.waitForStatus(task, s, TIMEOUT);
            }
        }

        public void waitForStatus(GraphExecutionState.Status s) throws InterruptedException {
            dao.waitForStatus(state.workflowId(), state.id(), s, TIMEOUT);
            Assert.assertEquals(s, dao.get(state.workflowId(), state.id()).status());
        }

        @Override
        public void close() throws InterruptedException {
            executor.gracefulStop();
            executor.join();
        }
    }

    public static Zygote buildZygote(List<String> inputs, List<String> outputs) {
        return new Zygote() {
            @Override
            public Slot[] input() {
                return inputs.stream().map(GraphExecutorTest::buildSlot).toArray(Slot[]::new);
            }

            @Override
            public Slot[] output() {
                return outputs.stream().map(GraphExecutorTest::buildSlot).toArray(Slot[]::new);
            }

            @Override
            public void run() {}

            @Override
            public String name() {
                return null;
            }
        };
    }

    public static Slot buildSlot(String name) {
        return new Slot() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public Media media() {
                return null;
            }

            @Override
            public Direction direction() {
                return null;
            }

            @Override
            public DataSchema contentType() {
                return null;
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
            for (String v: vertexes) {
                List<String> inputs = reversedEdges.getOrDefault(v, new ArrayList<>()).stream()
                    .map(s -> s + "to" + v).collect(Collectors.toList());
                List<String> outputs = edges.getOrDefault(v, new ArrayList<>()).stream()
                    .map(s -> v + "to" + s).collect(Collectors.toList());
                Zygote zygote = buildZygote(inputs, outputs);
                Map<String, String> slotsMapping = Stream.concat(inputs.stream(), outputs.stream())
                    .collect(Collectors.toMap(t -> t, t -> t));
                tasks.add(new TaskDescription(v, zygote, slotsMapping));
            }
            return new GraphDescription(tasks);
        }
    }
}
