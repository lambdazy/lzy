package ru.yandex.cloud.ml.platform.lzy.graph;

import io.grpc.ServerInterceptors;
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
import ru.yandex.cloud.ml.platform.lzy.graph.algo.GraphBuilder;
import ru.yandex.cloud.ml.platform.lzy.graph.algo.GraphBuilder.GraphValidationException;
import ru.yandex.cloud.ml.platform.lzy.graph.config.ServiceConfig;
import ru.yandex.cloud.ml.platform.lzy.graph.db.DaoException;
import ru.yandex.cloud.ml.platform.lzy.graph.db.GraphExecutionDao;
import ru.yandex.cloud.ml.platform.lzy.graph.queue.QueueManager;
import ru.yandex.cloud.ml.platform.lzy.model.logs.GrpcLogsInterceptor;
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
    private static final Logger LOG = LogManager.getLogger(GraphExecutorApi.class);
    private final GraphExecutionDao dao;
    private final GraphBuilder graphBuilder;
    private final ServiceConfig config;
    private final QueueManager queueManager;

    private Server server;

    @Inject
    public GraphExecutorApi(
            GraphExecutionDao dao,
            ServiceConfig config,
            GraphBuilder graphBuilder,
            QueueManager queueManager
    ) {
        this.dao = dao;
        this.config = config;
        this.graphBuilder = graphBuilder;
        this.queueManager = queueManager;
    }

    @Override
    public void execute(GraphExecuteRequest request, StreamObserver<GraphExecuteResponse> responseObserver) {
        final GraphDescription graph = GraphDescription.fromGrpc(request.getTasksList(), request.getChannelsList());
        try {
            graphBuilder.validate(graph);
        } catch (GraphValidationException e) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asException());
            return;
        }
        final GraphExecutionState graphExecution;
        try {
            graphExecution = queueManager.startGraph(request.getWorkflowId(), graph);
        } catch (StatusException e) {
            LOG.error("Cannot create graph for workflow <" + request.getWorkflowId() + ">", e);
            responseObserver.onError(e);
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
        } catch (DaoException e) {
            LOG.error("Cannot get status of graph {} in workflow {} from database",
                request.getGraphId(), request.getWorkflowId(), e
            );
            responseObserver.onError(Status.INTERNAL
                .withDescription("Cannot get graph execution status.").asException());
            return;
        }

        if (state == null) {
            responseObserver.onError(Status.NOT_FOUND
                .withDescription("DirectedGraph <" + request.getGraphId() + "> not found").asException());
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
        } catch (DaoException e) {

            LOG.error("Cannot get list of graphs in workflow {} from database", request.getWorkflowId(), e);
            responseObserver.onError(Status.INTERNAL
                .withDescription("Cannot get list of graph executions.").asException());
            return;
        }

        responseObserver.onNext(GraphListResponse.newBuilder().addAllGraphs(graphs).build());
        responseObserver.onCompleted();
    }

    @Override
    public void stop(GraphStopRequest request, StreamObserver<GraphStopResponse> responseObserver) {
        final GraphExecutionState state;
        try {
            state = queueManager.stopGraph(request.getWorkflowId(),
                request.getGraphId(), request.getIssue());
        } catch (StatusException e) {
            responseObserver.onError(e);
            return;
        }
        if (state == null) {
            responseObserver.onError(Status.NOT_FOUND
                .withDescription("DirectedGraph <" + request.getGraphId() + "> not found").asException());
            return;
        }
        responseObserver.onNext(GraphStopResponse.newBuilder().setStatus(state.toGrpc()).build());
        responseObserver.onCompleted();
    }

    public void close() {
        LOG.info("Closing GraphExecutor");
        if (server != null) {
            server.shutdown();
        }
        queueManager.gracefulShutdown();
    }

    public void start() throws InterruptedException, IOException {
        LOG.info("Starting GraphExecutor service...");

        queueManager.start();

        ServerBuilder<?> builder = NettyServerBuilder.forPort(config.port())
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES);

        builder.addService(ServerInterceptors.intercept(this, new GrpcLogsInterceptor()));
        server = builder.build();
        server.start();
    }

    public void awaitTermination() throws InterruptedException {
        server.awaitTermination();
        queueManager.join();
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
                api.awaitTermination();
            } catch (InterruptedException | IOException e) {
                LOG.error("Error while starting GraphExecutor", e);
            }
        }
    }
}
