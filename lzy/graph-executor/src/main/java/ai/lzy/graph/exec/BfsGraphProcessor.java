package ai.lzy.graph.exec;

import ai.lzy.graph.algo.Algorithms;
import ai.lzy.graph.algo.Algorithms.CondensedComponent;
import ai.lzy.graph.algo.Algorithms.CondensedGraph;
import ai.lzy.graph.algo.DirectedGraph;
import ai.lzy.graph.algo.GraphBuilder;
import ai.lzy.graph.algo.GraphBuilder.ChannelEdge;
import ai.lzy.graph.algo.GraphBuilder.TaskVertex;
import ai.lzy.graph.api.SchedulerApi;
import ai.lzy.graph.model.GraphExecutionState;
import ai.lzy.graph.model.GraphExecutionState.Status;
import ai.lzy.graph.model.TaskDescription;
import ai.lzy.graph.model.TaskExecution;
import ai.lzy.v1.scheduler.Scheduler;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Singleton
public class BfsGraphProcessor implements GraphProcessor {
    private static final Logger LOG = LogManager.getLogger(BfsGraphProcessor.class);

    private final SchedulerApi api;
    private final GraphBuilder graphBuilder;
    private final ChannelCheckerFactory checkerFactory;

    @Inject
    public BfsGraphProcessor(SchedulerApi api, GraphBuilder graphBuilder, ChannelCheckerFactory checkerFactory) {
        this.api = api;
        this.graphBuilder = graphBuilder;
        this.checkerFactory = checkerFactory;
    }

    @Override
    public GraphExecutionState exec(GraphExecutionState graph) {
        GraphExecutionState state = switch (graph.status()) {
            case WAITING -> nextStep(graph);
            case FAILED -> stop(graph, "Undefined state change to FAILED");
            case COMPLETED -> complete(graph);
            case EXECUTING -> {
                int completed = 0;
                for (TaskExecution task: graph.executions()) {
                    final Scheduler.TaskStatus status = api.status(graph.workflowId(), task.id());
                    if (status == null) {
                        LOG.error(String.format(
                            "TaskVertex <%s> not found in scheduler,"
                                + " but must be in executions of graph <%s>,"
                                + " stopping graph execution", task.id(), graph.id()
                        ));
                        yield stop(graph, "Internal error");
                    }
                    if (status.hasSuccess()) {
                        completed++;
                    }
                    if (status.hasError()) {
                        LOG.error("TaskVertex <" + task.id() + "> is in error state, stopping graph execution");
                        yield stop(graph, status.getError().getDescription(), task.id(),
                            task.description().operation().getName());
                    }
                }
                if (completed == graph.description().tasks().size()) {
                    yield complete(graph);
                }
                if (graph.currentExecutionGroup().isEmpty()) {
                    LOG.error("Some error while processing graph {}: executionGroup size is 0, but graph not completed",
                        graph.id());
                    yield stop(graph, "Some internal error");
                }
                yield nextStep(graph);
            }
        };
        LOG.debug(String.format(
            "DirectedGraph <%s> from workflow <%s> processed. DirectedGraph before processing: %s\n"
                + " DirectedGraph after processing: %s",
            graph.id(), graph.workflowId(), graph, state
        ));
        return state;
    }

    @Override
    public GraphExecutionState stop(GraphExecutionState graph, String errorDescription) {
        return stop(graph, errorDescription, null, null);
    }

    public GraphExecutionState stop(GraphExecutionState graph, String errorDescription,
                                    String failedTaskId, String failedTaskName)
    {
        return switch (graph.status()) {
            case COMPLETED, FAILED -> graph;
            case WAITING -> graph.copyFromThis()
                .withErrorDescription(errorDescription)
                .withFailedTaskId(failedTaskId)
                .withFailedTaskName(failedTaskName)
                .withStatus(Status.FAILED)
                .build();
            case EXECUTING -> {
                final var state = graph.copyFromThis()
                    .withErrorDescription(errorDescription)
                    .withFailedTaskId(failedTaskId)
                    .withFailedTaskName(failedTaskName)
                    .withStatus(Status.FAILED)
                    .build();
                graph.executions().forEach(t -> {
                    try {
                        api.kill(graph.workflowId(), t.id());
                    } catch (Exception e) {
                        LOG.error("Cannot stop graph task {} in scheduler", t.id(), e);
                    }
                });
                yield state;
            }
        };
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
                    LOG.info("Sending task {} from graph {} to scheduler", t.id(), graph.id());
                    var progress = api.execute(
                        graph.userId(), graph.workflowName(), graph.workflowId(), graph.allocatorSessionId(), t);
                    return new TaskExecution(progress.getTaskId(), t);
                })
                .collect(Collectors.toList());

            newExecutions.addAll(graph.executions());

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                // ignore
            }

            return graph.copyFromThis()
                .withExecutions(newExecutions)
                .withCurrentExecutionGroup(
                    newExecutions.stream()
                        .filter(t -> newExecutionGroup.contains(t.description()))
                        .collect(Collectors.toList())
                )
                .withStatus(Status.EXECUTING)
                .build();

        } catch (GraphBuilder.GraphValidationException e) {
            LOG.error("Error while planing next step of graph execution", e);
            return stop(graph, "Validation error: " + e.getMessage());
        }
    }

    private GraphExecutionState complete(GraphExecutionState graph) {
        LOG.info("Graph {} is completed", graph.id());
        return graph.copyFromThis()
            .withStatus(Status.COMPLETED)
            .build();
    }

    private Set<TaskDescription> getNextExecutionGroup(GraphExecutionState graphExecution)
        throws GraphBuilder.GraphValidationException
    {
        final DirectedGraph<TaskVertex, ChannelEdge> graph = graphBuilder.build(graphExecution.description());

        final CondensedGraph<TaskVertex, ChannelEdge> condensedGraph = Algorithms.condenseGraph(graph);

        final Map<String, TaskExecution> taskDescIdToTaskExec = graphExecution
            .executions()
            .stream()
            .collect(Collectors.toMap(t -> t.description().id(), t -> t));

        final Set<CondensedComponent<TaskVertex>> currentExecutionComponents;

        if (graphExecution.status() == Status.EXECUTING) {
            currentExecutionComponents = new HashSet<>();
            for (TaskExecution execution : graphExecution.currentExecutionGroup()) {
                currentExecutionComponents.add(
                    condensedGraph.vertexNameToComponentMap().get(execution.description().id())
                );
            }
        } else if (graphExecution.status() == Status.WAITING) {
            currentExecutionComponents = Algorithms.findRoots(condensedGraph);  // Graph is starting, return roots
            LOG.info("Starting execution of graph {} from workflow {}", graphExecution.id(),
                graphExecution.workflowId());
            return currentExecutionComponents.stream()
                .map(CondensedComponent::vertices)
                .flatMap(Collection::stream)
                .map(TaskVertex::description)
                .collect(Collectors.toSet());
        } else {
            throw new GraphBuilder.GraphValidationException(
                String.format("Invalid status of graph <%s>: %s", graphExecution.id(), graphExecution.status())
            );
        }

        final Set<CondensedComponent<TaskVertex>> alreadyProcessed = new HashSet<>();

        graphExecution
            .executions()
            .forEach(t -> alreadyProcessed.add(condensedGraph.vertexNameToComponentMap().get(t.description().id())));

        final Set<CondensedComponent<TaskVertex>> nextGroup = new HashSet<>();

        for (var comp: currentExecutionComponents) {  // Getting statuses of current group before next edges
            if (!comp.vertices().stream()
                .allMatch(
                    v -> {
                        final var exec = taskDescIdToTaskExec.get(v.description().id());
                        if (exec == null) {
                            return false;
                        }
                        final var status = api.status(graphExecution.workflowId(), exec.id());
                        return status != null && status.hasSuccess();
                    }))
            {
                nextGroup.add(comp);
            }
        }

        final var nextBfsGroup = Algorithms
            .getNextBfsGroup(condensedGraph, currentExecutionComponents.stream().toList())
            .stream()
            .filter(comp -> condensedGraph.parents(comp.name())
                .stream()
                .allMatch(
                    e -> e.condensedEdges()
                        .stream()
                        .allMatch(edge -> checkerFactory.checker(taskDescIdToTaskExec,
                            graphExecution.workflowId(), edge.channelDesc()).ready(edge))
                )
            )
            .filter(comp -> !alreadyProcessed.contains(comp))
            .collect(Collectors.toSet());

        nextGroup.addAll(nextBfsGroup);

        return nextGroup.stream()
            .map(CondensedComponent::vertices)
            .flatMap(Collection::stream)
            .map(TaskVertex::description)
            .collect(Collectors.toSet());
    }
}
