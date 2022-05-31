package ru.yandex.cloud.ml.platform.lzy.graph_executor.test;

import org.junit.Assert;
import org.junit.Test;
import ru.yandex.cloud.ml.platform.lzy.graph_executor.algo.BfsGraphBuilder;
import ru.yandex.cloud.ml.platform.lzy.graph_executor.algo.GraphBuilder;
import ru.yandex.cloud.ml.platform.lzy.graph_executor.model.GraphDescription;
import ru.yandex.cloud.ml.platform.lzy.graph_executor.model.GraphExecutionState;
import ru.yandex.cloud.ml.platform.lzy.graph_executor.model.TaskDescription;
import ru.yandex.cloud.ml.platform.lzy.graph_executor.model.TaskExecution;
import ru.yandex.cloud.ml.platform.lzy.graph_executor.test.mocks.SchedulerApiMock;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.Zygote;
import ru.yandex.cloud.ml.platform.lzy.model.data.DataSchema;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class BfsGraphBuilderTest {

    @Test
    public void testSimple() throws GraphBuilder.GraphValidationException {
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
        final SchedulerApiMock mock = new SchedulerApiMock();
        final GraphBuilder builder = new BfsGraphBuilder(mock);
        var l1 = builder.getNextExecutionGroup(new GraphExecutionState("", "", graph));
        Assert.assertEquals(Set.of("1", "3", "5", "7", "9"), Set.copyOf(l1));
        mock.changeStatus("1", SchedulerApiMock.EXECUTING);
        mock.changeStatus("3", SchedulerApiMock.QUEUE);
        mock.changeStatus("5", SchedulerApiMock.EXECUTING);
        mock.changeStatus("7", SchedulerApiMock.EXECUTING);
        mock.changeStatus("9", SchedulerApiMock.EXECUTING);

        GraphExecutionState exec2 = new GraphExecutionBuilder(graph)
            .addExecutions("1", "3", "5", "7", "9")
            .addExecutionGroup("1", "3", "5", "7", "9")
            .build();

        var l2 = builder.getNextExecutionGroup(exec2);
        Assert.assertEquals(Set.of("3", "6", "10"), Set.copyOf(l2));

        GraphExecutionState exec3 = new GraphExecutionBuilder(graph)
            .addExecutions("1", "3", "5", "7", "9", "6", "10")
            .addExecutionGroup("3", "6", "10")
            .build();

        mock.changeStatus("10", SchedulerApiMock.EXECUTING);
        mock.changeStatus("6", SchedulerApiMock.EXECUTING);

        var l3 = builder.getNextExecutionGroup(exec3);

        Assert.assertEquals(Set.of("3", "8"), Set.copyOf(l3));

        GraphExecutionState exec4 = new GraphExecutionBuilder(graph)
            .addExecutions("1", "3", "5", "6", "7", "8", "9", "10")
            .addExecutionGroup("3", "8")
            .build();

        mock.changeStatus("8", SchedulerApiMock.EXECUTING);
        mock.changeStatus("3", SchedulerApiMock.EXECUTING);

        var l4 = builder.getNextExecutionGroup(exec4);
        Assert.assertEquals(Set.of("2", "4"), Set.copyOf(l4));

        mock.changeStatus("2", SchedulerApiMock.EXECUTING);
        mock.changeStatus("4", SchedulerApiMock.EXECUTING);

        GraphExecutionState exec5 = new GraphExecutionBuilder(graph)
            .addExecutions("1", "2", "3", "4", "5", "6", "7", "8", "9", "10")
            .addExecutionGroup("2", "4")
            .build();

        var l5 = builder.getNextExecutionGroup(exec5);
        Assert.assertEquals(Set.of(), Set.copyOf(l5));
    }

    @Test
    public void testCycles() throws GraphBuilder.GraphValidationException {
        final GraphDescription graph = new GraphDescriptionBuilder()
            .addVertexes("1", "2", "3", "4", "5")
            .addEdge("1", "2")
            .addEdge("2", "1")
            .addEdge("1", "3")
            .addEdge("3", "4")
            .addEdge("4", "5")
            .addEdge("5", "3")
            .build();
        final SchedulerApiMock mock = new SchedulerApiMock();
        final GraphBuilder builder = new BfsGraphBuilder(mock);
        var l1 = builder.getNextExecutionGroup(new GraphExecutionState("", "", graph));
        Assert.assertEquals(Set.of("1", "2"), Set.copyOf(l1));

        GraphExecutionState exec2 = new GraphExecutionBuilder(graph)
            .addExecutions("1", "2")
            .addExecutionGroup("1", "2")
            .build();
        mock.changeStatus("1", SchedulerApiMock.EXECUTING);
        mock.changeStatus("2", SchedulerApiMock.QUEUE);
        var l2 = builder.getNextExecutionGroup(exec2);
        Assert.assertEquals(Set.of("1", "2"), Set.copyOf(l2));

        GraphExecutionState exec3 = new GraphExecutionBuilder(graph)
            .addExecutions("1", "2")
            .addExecutionGroup("1", "2")
            .build();
        mock.changeStatus("2", SchedulerApiMock.EXECUTING);
        var l3 = builder.getNextExecutionGroup(exec3);
        Assert.assertEquals(Set.of("3", "4", "5"), Set.copyOf(l3));

        mock.changeStatus("3", SchedulerApiMock.EXECUTING);
        mock.changeStatus("4", SchedulerApiMock.EXECUTING);
        mock.changeStatus("5", SchedulerApiMock.EXECUTING);
        GraphExecutionState exec4 = new GraphExecutionBuilder(graph)
            .addExecutions("1", "2", "3", "4", "5")
            .addExecutionGroup("3", "4", "5")
            .build();
        var l4 = builder.getNextExecutionGroup(exec4);
        Assert.assertEquals(Set.of(), Set.copyOf(l4));
    }

    public static Zygote buildZygote(List<String> inputs, List<String> outputs) {
        return new Zygote() {
            @Override
            public Slot[] input() {
                return inputs.stream().map(BfsGraphBuilderTest::buildSlot).toArray(Slot[]::new);
            }

            @Override
            public Slot[] output() {
                return outputs.stream().map(BfsGraphBuilderTest::buildSlot).toArray(Slot[]::new);
            }

            @Override
            public void run() {}
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

    public static class GraphExecutionBuilder {
        private final List<String> executions = new ArrayList<>();
        private final List<String> currentExecutionGroup = new ArrayList<>();
        private final GraphDescription graph;

        public GraphExecutionBuilder(GraphDescription graph) {
            this.graph = graph;
        }

        public GraphExecutionBuilder addExecutions(String... executions){
            this.executions.addAll(Arrays.stream(executions).toList());
            return this;
        }

        public GraphExecutionBuilder addExecutionGroup(String... executions){
            this.currentExecutionGroup.addAll(Arrays.stream(executions).toList());
            return this;
        }

        public GraphExecutionState build() {
            Map<String, TaskDescription> tasks = graph.tasks().stream()
                .collect(Collectors.toMap(TaskDescription::id, t -> t));
            List<TaskExecution> exec = executions.stream()
                .map(t -> new TaskExecution("", "", t, tasks.get(t)))
                .collect(Collectors.toList());
            List<TaskExecution> execGroup = currentExecutionGroup.stream()
                .map(t -> new TaskExecution("", "", t, tasks.get(t)))
                .collect(Collectors.toList());
            return new GraphExecutionState("", "", graph, exec, execGroup, GraphExecutionState.Status.EXECUTING);
        }

    }
}
