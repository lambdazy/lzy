package ru.yandex.cloud.ml.platform.lzy.graph.algo;

import jakarta.inject.Singleton;
import ru.yandex.cloud.ml.platform.lzy.graph.algo.DirectedGraph.Edge;
import ru.yandex.cloud.ml.platform.lzy.graph.model.GraphDescription;
import ru.yandex.cloud.ml.platform.lzy.graph.model.TaskDescription;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;

import java.util.*;
import java.util.stream.Collectors;

@Singleton
public class GraphBuilderImpl implements GraphBuilder {

    public DirectedGraph<TaskVertex> build(GraphDescription graphDescription) throws GraphValidationException {
        // TODO(artolord) add more validations
        final DirectedGraph<TaskVertex> graph = new DirectedGraph<>();
        final Map<String, ChannelDescription> channels = new HashMap<>();

        for (TaskDescription task : graphDescription.tasks()) {

            if (graph.vertexes().contains(new TaskVertex(task.id(), task))) {
                throw new GraphValidationException(
                    "TaskVertex with id <" + task.id() + "> was found many times in graph description"
                );
            }

            final Set<String> inputs = Set.copyOf(
                Arrays.stream(task.zygote().input())
                    .map(Slot::name)
                    .toList()
            );

            final Set<String> outputs = Set.copyOf(
                Arrays.stream(task.zygote().output())
                    .map(Slot::name)
                    .toList()
            );

            Set<String> intersection = new HashSet<>(inputs);
            intersection.retainAll(outputs);

            if (intersection.size() > 0) {
                throw new GraphValidationException(
                    String.format("Slots %s of task <%s> are inputs and outputs in the same time",
                        Arrays.toString(intersection.toArray()), task.id()
                    )
                );
            }

            for (Map.Entry<String, String> entry : task.slotsToChannelsAssignments().entrySet()) {

                ChannelDescription channel = channels.get(entry.getValue());
                if (channel == null) {
                    channel = new ChannelDescription();
                    channels.put(entry.getValue(), channel);
                }
                if (outputs.contains(entry.getKey())) {
                    channel.inputs().add(task);
                    continue;
                }
                if (inputs.contains(entry.getKey())) {
                    channel.outputs().add(task);
                    continue;
                }
                throw new GraphValidationException(
                    String.format(
                        "TaskVertex <%s> does not contains slot <%s>, but it was mentioned in slots assignments",
                        task.id(), entry.getKey()
                    )
                );
            }
        }
        for (Map.Entry<String, ChannelDescription> entry: channels.entrySet()) {
            final ChannelDescription channel = entry.getValue();
            final String channelName = entry.getKey();

            if (channel.inputs().isEmpty() || channel.outputs().isEmpty()) {
                throw new GraphValidationException(
                    String.format("Channel <%s> has no input or output slots",
                        channelName
                    )
                );
            }

            for (TaskDescription input: channel.inputs()) {
                graph.addEdges(
                    channel.outputs()
                        .stream()
                        .map(s -> new Edge<>(new TaskVertex(input.id(), input), new TaskVertex(s.id(), s)))
                        .collect(Collectors.toList())
                );
            }
        }
        return graph;
    }

    @Override
    public void validate(GraphDescription graph) throws GraphValidationException {
        if (graph.tasks().size() > MAX_VERTEXES) {
            throw new GraphValidationException("DirectedGraph is not valid: number of tasks is more then " + MAX_VERTEXES);
        }
        build(graph);
    }

    private record ChannelDescription(List<TaskDescription> inputs, List<TaskDescription> outputs) {
        ChannelDescription() {
            this(new ArrayList<>(), new ArrayList<>());
        }
    }

}
