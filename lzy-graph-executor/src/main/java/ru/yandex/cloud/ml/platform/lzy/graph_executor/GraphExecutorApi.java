package ru.yandex.cloud.ml.platform.lzy.graph_executor;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.ApplicationContext;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.graph_executor.algo.GraphBuilder;
import ru.yandex.cloud.ml.platform.lzy.graph_executor.algo.GraphBuilder.GraphValidationException;
import ru.yandex.cloud.ml.platform.lzy.graph_executor.api.SchedulerApi;
import ru.yandex.cloud.ml.platform.lzy.graph_executor.config.ServiceConfig;
import ru.yandex.cloud.ml.platform.lzy.graph_executor.db.GraphExecutionDao;
import ru.yandex.cloud.ml.platform.lzy.graph_executor.model.GraphDescription;
import ru.yandex.cloud.ml.platform.lzy.graph_executor.model.GraphExecutionState;
import ru.yandex.cloud.ml.platform.lzy.model.grpc.ChannelBuilder;
import yandex.cloud.priv.datasphere.v2.lzy.GraphExecutorApi.*;
import yandex.cloud.priv.datasphere.v2.lzy.GraphExecutorGrpc;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Singleton
public class GraphExecutorApi extends GraphExecutorGrpc.GraphExecutorImplBase {
    private final GraphExecutionDao graphDao;
    private final SchedulerApi schedulerApi;
    private final GraphBuilder graphBuilder;
    private final ServiceConfig config;
    private final Map<String, Map<String, GraphWatcher>> watchers = new HashMap<>();
    private static final Logger LOG = LogManager.getLogger(GraphExecutorApi.class);

    private Server server;

    @Inject
    public GraphExecutorApi(
            GraphExecutionDao graphDao,
            SchedulerApi schedulerApi,
            GraphBuilder graphBuilder,
            ServiceConfig config
    ) {
        this.graphDao = graphDao;
        this.schedulerApi = schedulerApi;
        this.graphBuilder = graphBuilder;
        this.config = config;
    }

    @Override
    public void execute(GraphExecuteRequest request, StreamObserver<GraphExecuteResponse> responseObserver) {
        final GraphDescription graph = GraphDescription.fromGrpc(request.getTasksList());
        try {
            graphBuilder.validate(graph);
        } catch (GraphValidationException e) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asException());
            return;
        }
        final GraphExecutionState graphExecution;
        try {
            graphExecution = graphDao.create(request.getWorkflowId(), graph);
        } catch (GraphExecutionDao.GraphDaoException e) {
            LOG.error("Cannot create graph for workflow <" + request.getWorkflowId() + ">", e);
            responseObserver.onError(Status.INTERNAL
                .withDescription("Cannot create graph. Please try again later.").asException());
            return;
        }
        scheduleGraphExecution(graphExecution);
        responseObserver.onNext(GraphExecuteResponse.newBuilder().setStatus(graphExecution.toGrpc()).build());
        responseObserver.onCompleted();
    }

    @Override
    public void status(GraphStatusRequest request, StreamObserver<GraphStatusResponse> responseObserver) {
        final GraphExecutionState state;
        try {
            state = graphDao.get(request.getWorkflowId(), request.getGraphId());
        } catch (GraphExecutionDao.GraphDaoException e) {
            LOG.error("Cannot get status of graph <"
                + request.getGraphId() + "> in workflow <"+ request.getWorkflowId() +"> from database", e);
            responseObserver.onError(Status.INTERNAL
                .withDescription("Cannot get graph execution status. Please try again later.").asException());
            return;
        }
        if (state == null) {
            responseObserver.onError(Status.NOT_FOUND
                .withDescription("Graph <" + request.getGraphId() + "> not found").asException());
            return;
        }

        responseObserver.onNext(GraphStatusResponse.newBuilder().setStatus(state.toGrpc()).build());
        responseObserver.onCompleted();
    }

    @Override
    public void list(GraphListRequest request, StreamObserver<GraphListResponse> responseObserver) {
        final List<GraphExecutionStatus> graphs;
        try {
            graphs = graphDao
                .list(request.getWorkflowId()).stream().map(GraphExecutionState::toGrpc).collect(Collectors.toList());
        } catch (GraphExecutionDao.GraphDaoException e) {
            LOG.error("Cannot get list of graphs in workflow <" +
                request.getWorkflowId() +"> from database", e);
            responseObserver.onError(Status.INTERNAL
                .withDescription("Cannot get list of graph executions. Please try again later.").asException());
            return;
        }
        responseObserver.onNext(GraphListResponse.newBuilder().addAllGraphs(graphs).build());
        responseObserver.onCompleted();
    }

    @Override
    public void stop(GraphStopRequest request, StreamObserver<GraphStopResponse> responseObserver) {
        final GraphWatcher watcher = watchers.getOrDefault(request.getWorkflowId(), new HashMap<>())
            .get(request.getGraphId());
        if (watcher == null) {
            final GraphExecutionState state;
            try {
                state = graphDao.get(request.getWorkflowId(), request.getGraphId());
            } catch (GraphExecutionDao.GraphDaoException e) {
                LOG.error("Cannot get status of graph <"
                        + request.getGraphId() + "> in workflow <"+ request.getWorkflowId() +"> from database", e);
                responseObserver.onError(Status.INTERNAL
                        .withDescription("Cannot stop graph execution. Please try again later.").asException());
                return;
            }
            if (state == null) {
                responseObserver.onError(Status.NOT_FOUND
                    .withDescription("Graph <" + request.getGraphId() + "> not found").asException());
                return;
            }
            if (state.status() != GraphExecutionState.Status.FAILED
                && state.status() != GraphExecutionState.Status.COMPLETED) {
                responseObserver.onError(Status.INTERNAL
                    .withDescription("Graph <" + request.getGraphId() + "> has no watcher, but not already completed")
                    .asException());
            }
            return;
        }
        try {
            watcher.stop(request.getIssue());
        } catch (StatusException e) {
            LOG.error("Cannot stop execution of graph <" +
                request.getGraphId() + "> in workflow <" + request.getWorkflowId() + ">", e);
            responseObserver.onError(e);
            return;
        }
        final GraphExecutionState state;
        try {
            state = graphDao.get(request.getWorkflowId(), request.getGraphId());
        } catch (GraphExecutionDao.GraphDaoException e) {
            LOG.error("Cannot get status of graph <"
                    + request.getGraphId() + "> in workflow <"+ request.getWorkflowId() +"> from database", e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Cannot stop graph execution. Please try again later.").asException());
            return;
        }
        assert state != null; // Unreachable
        responseObserver.onNext(GraphStopResponse.newBuilder().setStatus(state.toGrpc()).build());
        responseObserver.onCompleted();
    }

    public void close() {
        LOG.info("Closing GraphExecutor");
        if (server != null) {
            server.shutdown();
        }
        watchers.values().forEach(t -> t.values().forEach(TimerTask::cancel));
    }

    public void scheduleGraphExecution(GraphExecutionState state) {
        final GraphWatcher watcher = new GraphWatcher(
            state.workflowId(),
            state.id(),
            graphDao,
            schedulerApi,
            graphBuilder
        );
        watcher.run();
        watchers.computeIfAbsent(state.workflowId(), k -> new HashMap<>()).put(state.id(), watcher);
    }

    public void start() throws InterruptedException, IOException {
        LOG.info("Starting GraphExecutor service...");
        final List<GraphExecutionState> states;
        try {
            states = graphDao.filter(GraphExecutionState.Status.EXECUTING);
            states.addAll(
                graphDao.filter(GraphExecutionState.Status.WAITING)
            );
        } catch (GraphExecutionDao.GraphDaoException e) {
            LOG.error("Cannot restore state after restart", e);
            return;
        }

        states.forEach(this::scheduleGraphExecution);

        ServerBuilder<?> builder = NettyServerBuilder.forPort(config.port())
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES);

        builder.addService(this);
        server = builder.build();
        server.start();
        server.awaitTermination();
    }

    public static void main(String[] args) {
        try (ApplicationContext context = ApplicationContext.run()) {
            GraphExecutorApi api = context.getBean(GraphExecutorApi.class);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOG.info("Stopping GraphExecutor service");
                api.close();
            }));
            try {
                api.start();
            } catch (InterruptedException | IOException e) {
                LOG.error("Error while starting GraphExecutor", e);
            }
        }
    }
}
