package ai.lzy.graph.exec;

import ai.lzy.v1.SchedulerApi.TaskStatus;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import ai.lzy.v1.Tasks;

import java.util.*;
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
                    final TaskStatus status = api.status(graph.workflowId(), task.id());
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
            "DirectedGraph <%s> from workflow <%s> processed. DirectedGraph before processing: %s\n"
                + " DirectedGraph after processing: %s",
            graph.id(), graph.workflowId(), graph, state
        ));
        return state;
    }

    @Override
    public GraphExecutionState stop(GraphExecutionState graph, String errorDescription) {
        return switch (graph.status()) {
            case COMPLETED, FAILED -> graph;
            case WAITING -> graph.copyFromThis()
                .withErrorDescription(errorDescription)
                .withStatus(Status.FAILED)
                .build();
            case EXECUTING -> {
                graph.executions().forEach(t -> api.kill(graph.workflowId(), t.id()));
                yield graph.copyFromThis()
                    .withErrorDescription(errorDescription)
                    .withStatus(Status.FAILED)
                    .build();
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
                    final TaskStatus progress = api.execute(graph.workflowId(), t);
                    return new TaskExecution(progress.getTaskId(), t);
                })
                .collect(Collectors.toList());

            newExecutions.addAll(graph.executions());

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
        return graph.copyFromThis()
            .withStatus(Status.COMPLETED)
            .build();
    }

    private Set<TaskDescription> getNextExecutionGroup(GraphExecutionState graphExecution)
                                                                        throws GraphBuilder.GraphValidationException {
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
            currentExecutionComponents = Algorithms.findRoots(condensedGraph);
        } else {
            throw new GraphBuilder.GraphValidationException(
                String.format("Invalid status of graph <%s>: %s", graphExecution.id(), graphExecution.status())
            );
        }

        final Set<CondensedComponent<TaskVertex>> alreadyProcessed = new HashSet<>();

        graphExecution
            .executions()
            .forEach(t -> alreadyProcessed.add(condensedGraph.vertexNameToComponentMap().get(t.description().id())));

        final var nextBfsGroup = Algorithms
            .getNextBfsGroup(
                condensedGraph,
                currentExecutionComponents.stream().toList(),
                alreadyProcessed,
                t -> t.condensedEdges()
                        .stream()
                        .allMatch(edge -> {
                            ChannelChecker checker = checkerFactory.checker(taskDescIdToTaskExec,
                                graphExecution.workflowId(), edge.channelDesc());
                            return checker.ready(edge);
                        })
            );

        return nextBfsGroup.stream()
            .map(CondensedComponent::vertices)
            .flatMap(Collection::stream)
            .map(TaskVertex::description)
            .collect(Collectors.toSet());
    }
}
