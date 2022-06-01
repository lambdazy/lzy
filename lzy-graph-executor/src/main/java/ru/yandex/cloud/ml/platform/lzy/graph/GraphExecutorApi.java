package ru.yandex.cloud.ml.platform.lzy.graph;

import ru.yandex.cloud.ml.platform.lzy.graph.exec.GraphExecutor;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.ApplicationContext;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.graph.algo.GraphBuilder;
import ru.yandex.cloud.ml.platform.lzy.graph.algo.GraphBuilder.GraphValidationException;
import ru.yandex.cloud.ml.platform.lzy.graph.config.ServiceConfig;
import ru.yandex.cloud.ml.platform.lzy.graph.db.GraphExecutionDao;
import ru.yandex.cloud.ml.platform.lzy.graph.model.GraphDescription;
import ru.yandex.cloud.ml.platform.lzy.graph.model.GraphExecutionState;
import ru.yandex.cloud.ml.platform.lzy.model.grpc.ChannelBuilder;
import yandex.cloud.priv.datasphere.v2.lzy.GraphExecutorApi.*;
import yandex.cloud.priv.datasphere.v2.lzy.GraphExecutorGrpc;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Singleton
public class GraphExecutorApi extends GraphExecutorGrpc.GraphExecutorImplBase {
    private final GraphExecutor graphExecutor;
    private final GraphExecutionDao dao;
    private final GraphBuilder graphBuilder;
    private final ServiceConfig config;
    private static final Logger LOG = LogManager.getLogger(GraphExecutorApi.class);

    private Server server;

    @Inject
    public GraphExecutorApi(
            GraphExecutor graphExecutor,
            GraphExecutionDao dao,
            ServiceConfig config,
            GraphBuilder graphBuilder
    ) {
        this.graphExecutor = graphExecutor;
        this.dao = dao;
        this.config = config;
        this.graphBuilder = graphBuilder;
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
            graphExecution = dao.create(request.getWorkflowId(), graph);
        } catch (GraphExecutionDao.GraphDaoException e) {
            LOG.error("Cannot create graph for workflow <" + request.getWorkflowId() + ">", e);
            responseObserver.onError(Status.INTERNAL
                .withDescription("Cannot create graph. Please try again later.").asException());
            return;
        }
        responseObserver.onNext(GraphExecuteResponse.newBuilder().setStatus(graphExecution.toGrpc()).build());
        responseObserver.onCompleted();
    }

    @Override
    public void status(GraphStatusRequest request, StreamObserver<GraphStatusResponse> responseObserver) {
        final GraphExecutionState state;
        try {
            state = dao.get(request.getWorkflowId(), request.getGraphId());
        } catch (GraphExecutionDao.GraphDaoException e) {
            LOG.error(
                String.format("Cannot get status of graph <%s> in workflow <%s> from database",
                    request.getGraphId(), request.getWorkflowId()
                ),
                e
            );
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
            graphs = dao.list(request.getWorkflowId())
                .stream()
                .map(GraphExecutionState::toGrpc)
                .collect(Collectors.toList());
        } catch (GraphExecutionDao.GraphDaoException e) {
            LOG.error(
                String.format("Cannot get list of graphs in workflow <%s> from database", request.getWorkflowId()),
                e
            );
            responseObserver.onError(Status.INTERNAL
                .withDescription("Cannot get list of graph executions. Please try again later.").asException());
            return;
        }
        responseObserver.onNext(GraphListResponse.newBuilder().addAllGraphs(graphs).build());
        responseObserver.onCompleted();
    }

    @Override
    public void stop(GraphStopRequest request, StreamObserver<GraphStopResponse> responseObserver) {
        try {
            dao.updateAtomic(request.getWorkflowId(), request.getGraphId(), state -> {
                if (state == null) {
                    responseObserver.onError(Status.NOT_FOUND
                        .withDescription("Graph <" + request.getGraphId() + "> not found").asException());
                    return null;
                }
                if (Set.of(
                    GraphExecutionState.Status.COMPLETED,
                    GraphExecutionState.Status.FAILED,
                    GraphExecutionState.Status.SCHEDULED_TO_FAIL
                ).contains(state.status())) {
                    responseObserver.onNext(GraphStopResponse.newBuilder().setStatus(state.toGrpc()).build());
                    responseObserver.onCompleted();
                    return state;
                }
                GraphExecutionState newState = new GraphExecutionState(
                    state.workflowId(),
                    state.id(),
                    state.description(),
                    state.executions(),
                    state.currentExecutionGroup(),
                    GraphExecutionState.Status.SCHEDULED_TO_FAIL,
                    "Stopped from outside:" + request.getIssue()
                );
                responseObserver.onNext(GraphStopResponse.newBuilder().setStatus(newState.toGrpc()).build());
                responseObserver.onCompleted();
                return newState;
            });
        } catch (GraphExecutionDao.GraphDaoException e) {
            LOG.error(String.format(
                "Cannot update status of graph <%s> in workflow <%s> from database",
                request.getGraphId(), request.getWorkflowId()
            ), e);
            responseObserver.onError(Status.INTERNAL
                .withDescription("Cannot stop graph execution. Please try again later.").asException());
        }
    }

    public void close() {
        LOG.info("Closing GraphExecutor");
        if (server != null) {
            server.shutdown();
        }
        graphExecutor.gracefulStop();
    }

    public void start() throws InterruptedException, IOException {
        LOG.info("Starting GraphExecutor service...");

        graphExecutor.start();

        ServerBuilder<?> builder = NettyServerBuilder.forPort(config.port())
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES);

        builder.addService(this);
        server = builder.build();
        server.start();
        server.awaitTermination();
        graphExecutor.join();
    }

    public static void main(String[] args) {
        try (final ApplicationContext context = ApplicationContext.run()) {
            final GraphExecutorApi api = context.getBean(GraphExecutorApi.class);
            final Thread mainThread = Thread.currentThread();

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOG.info("Stopping GraphExecutor service");
                api.close();
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    LOG.error(e);
                }
            }));
            try {
                api.start();
            } catch (InterruptedException | IOException e) {
                LOG.error("Error while starting GraphExecutor", e);
            }
        }
    }
}
