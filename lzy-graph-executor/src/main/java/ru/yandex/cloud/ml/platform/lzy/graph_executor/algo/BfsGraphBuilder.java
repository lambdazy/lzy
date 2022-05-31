package ru.yandex.cloud.ml.platform.lzy.graph_executor.algo;

import jakarta.inject.Singleton;
import ru.yandex.cloud.ml.platform.lzy.graph_executor.api.SchedulerApi;
import ru.yandex.cloud.ml.platform.lzy.graph_executor.model.GraphDescription;
import ru.yandex.cloud.ml.platform.lzy.graph_executor.model.GraphExecutionState;
import ru.yandex.cloud.ml.platform.lzy.graph_executor.model.TaskDescription;
import ru.yandex.cloud.ml.platform.lzy.graph_executor.model.TaskExecution;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import yandex.cloud.priv.datasphere.v2.lzy.Tasks;

import java.util.*;
import java.util.stream.Collectors;

@Singleton
public class BfsGraphBuilder implements GraphBuilder{
    private final SchedulerApi schedulerApi;

    public BfsGraphBuilder(SchedulerApi schedulerApi) {
        this.schedulerApi = schedulerApi;
    }

    private Graph build(GraphDescription graphDescription) throws GraphValidationException {
        // TODO(artolord) add more validations
        final Graph graph = new Graph();
        final Map<String, ChannelDescription> channels = new HashMap<>();
        for (TaskDescription task : graphDescription.tasks()) {
            if (graph.vertexes().contains(task.id())) {
                throw new GraphValidationException(
                    "Task with id <" + task.id() + "> was found many times in graph description"
                );
            }
            final Set<String> inputs = Set.copyOf(Arrays.stream(task.zygote().input()).map(Slot::name).toList());
            final Set<String> outputs = Set.copyOf(Arrays.stream(task.zygote().output()).map(Slot::name).toList());
            for (Map.Entry<String, String> entry : task.slotsToChannelsAssignments().entrySet()) {
                ChannelDescription channel = channels.get(entry.getValue());
                if (channel == null) {
                    channel = new ChannelDescription();
                    channels.put(entry.getValue(), channel);
                }
                if (outputs.contains(entry.getKey())) {
                    channel.inputs.add(task.id());
                    continue;
                }
                if (inputs.contains(entry.getKey())) {
                    channel.outputs.add(task.id());
                    continue;
                }
                throw new GraphValidationException(
                    "Task <" +
                    task.id() +
                    "> does not contains slot <" + entry.getKey() + ">, but it was mentioned in slots assignments"
                );
            }
        }
        for (ChannelDescription channel: channels.values()) {
            for (String input: channel.inputs) {
                graph.addEdges(
                    channel.outputs.stream()
                        .map(s -> new Graph.Edge(input, s))
                        .collect(Collectors.toList())
                );
            }
        }
        return graph;
    }

    @Override
    public Set<String> getNextExecutionGroup(GraphExecutionState graphExecution) throws GraphValidationException {
        final Graph graph = build(graphExecution.description());
        final Algorithms.CondensedGraph condensedGraph = Algorithms.condenseGraph(graph);
        final Map<String, TaskExecution> taskDescIdToTaskExec = graphExecution.executions().stream()
            .collect(Collectors.toMap(t -> t.description().id(), t -> t));
        final Map<String, Set<String>> componentToTasks = new HashMap<>();
        for (Map.Entry<String, String> entry : condensedGraph.vertexToComponentMapping().entrySet()) {
            componentToTasks.computeIfAbsent(entry.getValue(), k -> new HashSet<>()).add(entry.getKey());
        }
        final Set<String> currentExecutionComponents = new HashSet<>();
        for (TaskExecution execution: graphExecution.currentExecutionGroup()) {
            currentExecutionComponents.add(
                condensedGraph.vertexToComponentMapping().get(execution.description().id())
            );
        }
        List<String> nextBfsGroup = Algorithms
            .getNextBfsGroup(condensedGraph.graph(), currentExecutionComponents.stream().toList(), t -> {
                final Set<String> tasks = componentToTasks.get(t);
                boolean canHasChildren = true;
                for (String task: tasks) {
                    if (!taskDescIdToTaskExec.containsKey(task)) {
                        canHasChildren = false;
                        break;
                    }
                    final TaskExecution exec = taskDescIdToTaskExec.get(task);
                    Tasks.TaskProgress progress = schedulerApi.status(exec.workflowId(), exec.id());
                    if (
                        progress == null ||
                        progress.getStatus().getNumber() < Tasks.TaskProgress.Status.EXECUTING.getNumber() ||
                        progress.getStatus() == Tasks.TaskProgress.Status.ERROR
                    ) {
                        canHasChildren = false;
                        break;
                    }
                }
                return canHasChildren;
            });
        return nextBfsGroup.stream()
            .map(componentToTasks::get)
            .flatMap(Collection::stream)
            .collect(Collectors.toSet());
    }

    @Override
    public void validate(GraphDescription graph) throws GraphValidationException {
        if (graph.tasks().size() > MAX_VERTEXES) {
            throw new GraphValidationException("Graph is not valid: number of tasks is more then " + MAX_VERTEXES);
        }
        build(graph);
    }

    private static class ChannelDescription {
        private final List<String> inputs = new ArrayList<>();
        private final List<String> outputs = new ArrayList<>();
    }
}
