package ru.yandex.cloud.ml.platform.lzy.graph_executor;

import io.grpc.Status;
import io.grpc.StatusException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.graph_executor.algo.GraphBuilder;
import ru.yandex.cloud.ml.platform.lzy.graph_executor.api.SchedulerApi;
import ru.yandex.cloud.ml.platform.lzy.graph_executor.db.GraphExecutionDao;
import ru.yandex.cloud.ml.platform.lzy.graph_executor.model.GraphExecutionState;
import ru.yandex.cloud.ml.platform.lzy.graph_executor.model.TaskExecution;
import yandex.cloud.priv.datasphere.v2.lzy.Tasks;

import java.util.*;
import java.util.stream.Collectors;

public class GraphWatcher extends TimerTask {
    public static final int DEFAULT_PERIOD = 1000;  // 1s
    private final Timer ttl;
    private final String workflowId;
    private final String graphExecutionId;
    private final GraphExecutionDao graphDao;
    private final SchedulerApi api;
    private final GraphBuilder graphBuilder;
    private final static Logger LOG = LogManager.getLogger(GraphWatcher.class);

    public GraphWatcher(String workflowId, String graphExecutionId,
            GraphExecutionDao graphDao, SchedulerApi api, GraphBuilder graphBuilder, int periodMillis) {
        this.ttl = new Timer("Graph watcher timer for graph <" + graphExecutionId + ">", true);
        this.workflowId = workflowId;
        this.graphExecutionId = graphExecutionId;
        this.graphDao = graphDao;
        this.api = api;
        this.graphBuilder = graphBuilder;
        ttl.scheduleAtFixedRate(this, periodMillis, periodMillis);
    }

    public GraphWatcher(String workflowId, String graphExecutionId,
            GraphExecutionDao graphDao, SchedulerApi api, GraphBuilder graphBuilder) {
        this(workflowId, graphExecutionId, graphDao, api, graphBuilder, DEFAULT_PERIOD);
    }

    @Override
    public void run() {
        try {
            graphDao.updateAtomic(workflowId, graphExecutionId, graph -> {
                if (graph == null) {
                    ttl.cancel();
                    RuntimeException e = new RuntimeException("Cannot find graph <" + workflowId +
                        "> with workflow id <" + graphExecutionId + "> for watcher. Destroying graphWatcher"
                    );
                    LOG.error(e);
                    throw e;
                }
                GraphExecutionState state = switch (graph.status()) {
                    case WAITING -> start(graph);
                    case FAILED -> stop(graph, "Undefined state change to ERRORED");
                    case COMPLETED -> complete(graph);
                    case EXECUTING -> {
                        int completed = 0;
                        for (TaskExecution task: graph.executions()) {
                            final Tasks.TaskProgress progress = api.status(task.workflowId(), task.id());
                            if (progress == null) {
                                LOG.error("Task <" + task.id() +
                                    "> not found in scheduler, but must be in executions of graph <" + graph.id() + ">," +
                                    " stopping graph execution");
                                yield stop(graph, "Internal error");
                            }
                            if (progress.getStatus() == Tasks.TaskProgress.Status.SUCCESS) {
                                completed ++;
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
                LOG.debug("Graph <" + graphExecutionId + "> processed. Graph before processing: " + graph + "\n Graph after processing: " + state);
                return state;
            });
        } catch (GraphExecutionDao.GraphDaoException e) {
            LOG.error("Cannot update state of graph <" + graphExecutionId + ">", e);
            try {
                // Trying to stop graph
                stop("Internal: Cannot save state of graph execution. Destroying it");
            } catch (StatusException ex) {
                LOG.error("Graph <" + graphExecutionId + "> is not in valid state. Stopping watcher.");
                ttl.cancel();
                throw new IllegalStateException(ex);
            }
        }
    }

    public void stop(String errorDescription) throws StatusException {
        try {
            graphDao.updateAtomic(workflowId, graphExecutionId, graph -> {
                if (graph == null) {
                    ttl.cancel();
                    StatusException e = Status.NOT_FOUND.withDescription("Cannot find graph <" + workflowId +
                        "> with workflow id <" + graphExecutionId + "> for watcher. Destroying graphWatcher"
                    ).asException();
                    LOG.error(e);
                    throw e;
                }
                return stop(graph,
                        "Stopped by user with error description: " + errorDescription);
            });
        } catch (GraphExecutionDao.GraphDaoException e) {
            LOG.error("Error while stopping graph <" + graphExecutionId + ">", e);
            throw Status.INTERNAL
                .withDescription("Cannot stop graph <" + graphExecutionId + ">, please try again").asException();
        }
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
                    final Tasks.TaskProgress progress = api.execute(workflowId, t);
                    return new TaskExecution(workflowId, graphExecutionId, progress.getTid(), t);
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

    private GraphExecutionState stop(GraphExecutionState graph, String errorDescription) {
        ttl.cancel();
        graph.executions().forEach(t -> api.kill(workflowId, t.id()));
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

    private GraphExecutionState complete(GraphExecutionState graph) {
        ttl.cancel();
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
