package ai.lzy.graph.algo;

import ai.lzy.graph.model.ChannelDescription;
import ai.lzy.graph.model.GraphDescription;
import ai.lzy.graph.model.TaskDescription;
import ai.lzy.v1.common.LMS;
import jakarta.inject.Singleton;

import java.util.*;

@Singleton
public class GraphBuilderImpl implements GraphBuilder {

    public DirectedGraph<TaskVertex, ChannelEdge> build(GraphDescription graphDescription)
        throws GraphValidationException
    {
        // TODO(artolord) add more validations
        final DirectedGraph<TaskVertex, ChannelEdge> graph = new DirectedGraph<>();
        final Map<String, ChannelHolder> channels = new HashMap<>();

        for (TaskDescription task : graphDescription.tasks()) {

            if (graph.vertexes().contains(new TaskVertex(task.id(), task))) {
                throw new GraphValidationException(
                    "TaskVertex with id <" + task.id() + "> was found many times in graph description"
                );
            }

            final Set<String> inputs = Set.copyOf(
                task.operation().getSlotsList().stream()
                    .filter(s -> s.getDirection() == LMS.Slot.Direction.INPUT)
                    .map(LMS.Slot::getName)
                    .toList()
            );

            final Set<String> outputs = Set.copyOf(
                task.operation().getSlotsList().stream()
                    .filter(s -> s.getDirection() == LMS.Slot.Direction.OUTPUT)
                    .map(LMS.Slot::getName)
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

            graph.addVertex(new TaskVertex(task.id(), task));

            for (Map.Entry<String, String> entry : task.slotsToChannelsAssignments().entrySet()) {

                ChannelHolder channel = channels.get(entry.getValue());
                if (channel == null) {
                    ChannelDescription desc = graphDescription.channels().get(entry.getValue());
                    channel = new ChannelHolder(
                        Objects.requireNonNullElseGet(desc, () -> new ChannelDescription(entry.getValue())));
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
        for (Map.Entry<String, ChannelHolder> entry: channels.entrySet()) {
            final ChannelHolder channel = entry.getValue();

            if (channel.inputs().isEmpty() || channel.outputs().isEmpty()) {
                continue; // Skipping external channels
            }

            graph.addEdges(entry.getValue().edges());
        }
        return graph;
    }

    @Override
    public void validate(GraphDescription graph) throws GraphValidationException {
        if (graph.tasks().size() > MAX_VERTEXES) {
            throw new GraphValidationException(
                "DirectedGraph is not valid: number of tasks is more then " + MAX_VERTEXES
            );
        }
        build(graph);
    }

    private record ChannelHolder(
        List<TaskDescription> inputs,
        List<TaskDescription> outputs,
        ChannelDescription channelDescription
    ) {
        ChannelHolder(ChannelDescription channelDescription) {
            this(new ArrayList<>(), new ArrayList<>(), channelDescription);
        }

        public List<ChannelEdge> edges() {
            final List<ChannelEdge> edges = new ArrayList<>();
            for (TaskDescription input: inputs) {
                for (TaskDescription output: outputs) {
                    edges.add(new ChannelEdge(
                        new TaskVertex(input.id(), input),
                        new TaskVertex(output.id(), output),
                        channelDescription));
                }
            }
            return edges;
        }

    }
}
