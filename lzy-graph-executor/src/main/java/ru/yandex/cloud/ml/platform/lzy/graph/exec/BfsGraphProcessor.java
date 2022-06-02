package ru.yandex.cloud.ml.platform.lzy.graph.exec;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.graph.algo.Algorithms;
import ru.yandex.cloud.ml.platform.lzy.graph.algo.Algorithms.CondensedComponent;
import ru.yandex.cloud.ml.platform.lzy.graph.algo.Graph;
import ru.yandex.cloud.ml.platform.lzy.graph.algo.GraphBuilder;
import ru.yandex.cloud.ml.platform.lzy.graph.algo.GraphBuilder.TaskVertex;
import ru.yandex.cloud.ml.platform.lzy.graph.api.SchedulerApi;
import ru.yandex.cloud.ml.platform.lzy.graph.model.GraphExecutionState;
import ru.yandex.cloud.ml.platform.lzy.graph.model.TaskDescription;
import ru.yandex.cloud.ml.platform.lzy.graph.model.TaskExecution;
import yandex.cloud.priv.datasphere.v2.lzy.Tasks;

import java.util.*;
import java.util.stream.Collectors;

@Singleton
public class BfsGraphProcessor implements GraphProcessor {
    private static final Logger LOG = LogManager.getLogger(BfsGraphProcessor.class);

    private final SchedulerApi api;
    private final GraphBuilder graphBuilder;

    @Inject
    public BfsGraphProcessor(SchedulerApi api, GraphBuilder graphBuilder) {
        this.api = api;
        this.graphBuilder = graphBuilder;
    }

    @Override
    public GraphExecutionState exec(GraphExecutionState graph) {
        GraphExecutionState state = switch (graph.status()) {
            case WAITING -> nextStep(graph);
            case FAILED -> stop(graph, "Undefined state change to FAILED");
            case SCHEDULED_TO_FAIL -> stop(graph, graph.errorDescription());
            case COMPLETED -> complete(graph);
            case EXECUTING -> {
                int completed = 0;
                for (TaskExecution task: graph.executions()) {
                    final Tasks.TaskProgress progress = api.status(graph.workflowId(), task.id());
                    if (progress == null) {
                        LOG.error(String.format(
                            "TaskVertex <%s> not found in scheduler,"
                                + " but must be in executions of graph <%s>,"
                                + " stopping graph execution", task.id(), graph.id()
                        ));
                        yield stop(graph, "Internal error");
                    }
                    if (progress.getStatus() == Tasks.TaskProgress.Status.SUCCESS) {
                        completed++;
                    }
                    if (progress.getStatus() == Tasks.TaskProgress.Status.ERROR) {
                        LOG.error("TaskVertex <" + task.id() + "> is in error state, stopping graph execution");
                        yield stop(graph, "TaskVertex <" + task.id() + "> is in error state");
                    }
                }
                if (completed == graph.description().tasks().size()) {
                    yield complete(graph);
                }
                yield nextStep(graph);
            }
        };
        LOG.debug(String.format(
            "Graph <%s> from workflow <%s> processed. Graph before processing: %s\n"
                + " Graph after processing: %s",
            graph.id(), graph.workflowId(), graph, state
        ));
        return state;
    }

    private GraphExecutionState stop(GraphExecutionState graph, String errorDescription) {
        graph.executions().forEach(t -> api.kill(graph.workflowId(), t.id()));
        return new GraphExecutionState(
            graph.workflowId(),
            graph.id(),
            graph.description(),
            graph.executions(),
            graph.currentExecutionGroup(),
            GraphExecutionState.Status.FAILED,
            errorDescription
        );
    }

    private GraphExecutionState nextStep(GraphExecutionState graph) {
        try {
            final Set<TaskDescription> newExecutionGroup = getNextExecutionGroup(graph);
            final Set<TaskDescription> oldExecutionGroup = graph.currentExecutionGroup()
                .stream()
                .map(TaskExecution::description)
                .collect(Collectors.toSet());

            final Set<TaskDescription> diff = new HashSet<>(newExecutionGroup);
            diff.removeAll(oldExecutionGroup);
            final List<TaskExecution> newExecutions = diff
                .stream()
                .map(t -> {
                    final Tasks.TaskProgress progress = api.execute(graph.workflowId(), t);
                    return new TaskExecution(progress.getTid(), t);
                })
                .collect(Collectors.toList());

            newExecutions.addAll(graph.executions());

            return new GraphExecutionState(
                graph.workflowId(),
                graph.id(),
                graph.description(),
                newExecutions,
                newExecutions.stream()
                    .filter(t -> newExecutionGroup.contains(t.description()))
                    .collect(Collectors.toList()),
                GraphExecutionState.Status.EXECUTING);

        } catch (GraphBuilder.GraphValidationException e) {
            LOG.error("Error while planing next step of graph execution", e);
            return stop(graph, "Validation error: " + e.getMessage());
        }
    }

    private GraphExecutionState complete(GraphExecutionState graph) {
        return changeState(graph, GraphExecutionState.Status.COMPLETED);
    }

    private GraphExecutionState changeState(GraphExecutionState graph, GraphExecutionState.Status status) {
        return new GraphExecutionState(
            graph.workflowId(),
            graph.id(),
            graph.description(),
            graph.executions(),
            graph.currentExecutionGroup(),
            status
        );
    }

    private Set<TaskDescription> getNextExecutionGroup(GraphExecutionState graphExecution)
                                                                        throws GraphBuilder.GraphValidationException {
        final Graph<TaskVertex> graph = graphBuilder.build(graphExecution.description());

        final Algorithms.CondensedGraph<TaskVertex> condensedGraph = Algorithms.condenseGraph(graph);

        final Map<String, TaskExecution> taskDescIdToTaskExec = graphExecution
            .executions()
            .stream()
            .collect(Collectors.toMap(t -> t.description().id(), t -> t));

        final Set<CondensedComponent<TaskVertex>> currentExecutionComponents;

        if (graphExecution.status() == GraphExecutionState.Status.EXECUTING) {
            currentExecutionComponents = new HashSet<>();
            for (TaskExecution execution : graphExecution.currentExecutionGroup()) {
                currentExecutionComponents.add(
                    condensedGraph.vertexNameToComponentMap().get(execution.description().id())
                );
            }
        } else if (graphExecution.status() == GraphExecutionState.Status.WAITING) {
            currentExecutionComponents = Algorithms.findRoots(condensedGraph);
        } else {
            throw new GraphBuilder.GraphValidationException(
                String.format("Invalid status of graph <%s>: %s", graphExecution.id(), graphExecution.status())
            );
        }

        final var nextBfsGroup = Algorithms
            .getNextBfsGroup(
                condensedGraph,
                currentExecutionComponents.stream().toList(),
                t -> {
                    final Set<TaskVertex> tasks = t.vertices();

                    boolean canHasChildren = true;
                    for (TaskVertex task: tasks) {
                        if (!taskDescIdToTaskExec.containsKey(task.description().id())) {
                            canHasChildren = false;
                            break;
                        }
                        final TaskExecution exec = taskDescIdToTaskExec.get(task.description().id());
                        Tasks.TaskProgress progress = api.status(graphExecution.workflowId(), exec.id());
                        if (
                            progress == null
                                || progress.getStatus().getNumber() < Tasks.TaskProgress.Status.EXECUTING.getNumber()
                                || progress.getStatus() == Tasks.TaskProgress.Status.ERROR
                        ) {
                            canHasChildren = false;
                            break;
                        }
                    }
                    return canHasChildren;
                });

        return nextBfsGroup.stream()
            .map(CondensedComponent::vertices)
            .flatMap(Collection::stream)
            .map(TaskVertex::description)
            .collect(Collectors.toSet());
    }
}
