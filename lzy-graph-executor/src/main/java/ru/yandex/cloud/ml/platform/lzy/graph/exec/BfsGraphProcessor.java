package ru.yandex.cloud.ml.platform.lzy.graph.exec;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.graph.algo.GraphBuilder;
import ru.yandex.cloud.ml.platform.lzy.graph.api.SchedulerApi;
import ru.yandex.cloud.ml.platform.lzy.graph.model.GraphExecutionState;
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
            case WAITING -> start(graph);
            case FAILED -> stop(graph, "Undefined state change to FAILED");
            case SCHEDULED_TO_FAIL -> stop(graph, graph.errorDescription());
            case COMPLETED -> complete(graph);
            case EXECUTING -> {
                int completed = 0;
                for (TaskExecution task: graph.executions()) {
                    final Tasks.TaskProgress progress = api.status(task.workflowId(), task.id());
                    if (progress == null) {
                        LOG.error(String.format(
                            "Task <%s> not found in scheduler,"
                                + " but must be in executions of graph <%s>,"
                                + " stopping graph execution", task.id(), task.graphExecutionId()
                        ));
                        yield stop(graph, "Internal error");
                    }
                    if (progress.getStatus() == Tasks.TaskProgress.Status.SUCCESS) {
                        completed++;
                    }
                    if (progress.getStatus() == Tasks.TaskProgress.Status.ERROR) {
                        LOG.error("Task <" + task.id() + "> is in error state, stopping graph execution");
                        yield stop(graph, "Task <" + task.id() + "> is in error state");
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

    private GraphExecutionState start(GraphExecutionState graph) {
        return changeState(graph, GraphExecutionState.Status.EXECUTING);
    }

    private GraphExecutionState nextStep(GraphExecutionState graph) {
        try {
            final Set<String> newExecutionGroup = graphBuilder.getNextExecutionGroup(graph);
            final Set<String> oldExecutionGroup = graph.currentExecutionGroup()
                .stream()
                .map(TaskExecution::id)
                .collect(Collectors.toSet());

            final Set<String> diff = new HashSet<>(newExecutionGroup);
            diff.removeAll(oldExecutionGroup);
            final List<TaskExecution> newExecutions = graph.description().tasks().stream()
                .filter(t -> diff.contains(t.id()))
                .map(t -> {
                    final Tasks.TaskProgress progress = api.execute(graph.workflowId(), t);
                    return new TaskExecution(graph.workflowId(), graph.id(), progress.getTid(), t);
                })
                .collect(Collectors.toList());

            newExecutions.addAll(graph.executions());

            return new GraphExecutionState(
                graph.workflowId(),
                graph.id(),
                graph.description(),
                newExecutions,
                newExecutions.stream()
                    .filter(t -> newExecutionGroup.contains(t.description().id()))
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
}
