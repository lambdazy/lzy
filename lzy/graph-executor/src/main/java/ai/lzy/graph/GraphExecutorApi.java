package ai.lzy.graph;

import ai.lzy.graph.algo.GraphBuilder;
import ai.lzy.graph.api.SchedulerApi;
import ai.lzy.graph.config.ServiceConfig;
import ai.lzy.graph.db.GraphExecutionDao;
import ai.lzy.graph.model.GraphDescription;
import ai.lzy.graph.model.GraphExecutionState;
import ai.lzy.graph.queue.QueueManager;
import ai.lzy.iam.grpc.client.AuthenticateServiceGrpcClient;
import ai.lzy.iam.grpc.interceptors.AllowInternalUserOnlyInterceptor;
import ai.lzy.iam.grpc.interceptors.AuthServerInterceptor;
import ai.lzy.longrunning.IdempotencyUtils;
import ai.lzy.longrunning.Operation;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.exceptions.DaoException;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.v1.graph.GraphExecutor;
import ai.lzy.v1.graph.GraphExecutorApi.*;
import ai.lzy.v1.graph.GraphExecutorGrpc;
import com.google.protobuf.Any;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.ApplicationContext;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

import static ai.lzy.longrunning.IdempotencyUtils.handleIdempotencyKeyConflict;
import static ai.lzy.longrunning.IdempotencyUtils.loadExistingOpResult;
import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcServer;
import static ai.lzy.util.grpc.ProtoConverter.toProto;

@Singleton
public class GraphExecutorApi extends GraphExecutorGrpc.GraphExecutorImplBase {
    private static final Logger LOG = LogManager.getLogger(GraphExecutorApi.class);

    public static final String APP = "LzyGraphExecutor";

    private final ManagedChannel iamChannel;
    private final GraphExecutionDao dao;
    private final OperationDao operationDao;
    private final GraphBuilder graphBuilder;
    private final ServiceConfig config;
    private final QueueManager queueManager;
    private final SchedulerApi schedulerApi;

    private Server server;

    @Inject
    public GraphExecutorApi(ServiceConfig config, GraphExecutionDao dao,
                            @Named("GraphExecutorOperationDao") OperationDao operationDao,
                            @Named("GraphExecutorIamGrpcChannel") ManagedChannel iamChannel,
                            @Named("GraphExecutorIamToken") RenewableJwt iamToken,
                            GraphBuilder graphBuilder, QueueManager queueManager, SchedulerApi schedulerApi)
    {
        this.iamChannel = iamChannel;
        this.dao = dao;
        this.operationDao = operationDao;
        this.config = config;
        this.graphBuilder = graphBuilder;
        this.queueManager = queueManager;
        this.schedulerApi = schedulerApi;
    }

    @Override
    public void execute(GraphExecuteRequest request, StreamObserver<GraphExecuteResponse> responseObserver) {
        if (!validateRequest(request, responseObserver)) {
            return;
        }

        Operation.IdempotencyKey idempotencyKey = IdempotencyUtils.getIdempotencyKey(request);
        if (idempotencyKey != null &&
            loadExistingOpResult(operationDao, idempotencyKey, responseObserver, GraphExecuteResponse.class,
                Duration.ofMillis(100), Duration.ofSeconds(5), LOG))
        {
            return;
        }

        final GraphDescription graph = GraphDescription.fromGrpc(request.getTasksList(), request.getChannelsList());
        try {
            graphBuilder.validate(graph);
        } catch (GraphBuilder.GraphValidationException e) {
            LOG.error("Graph did not pass validation: { graph: {}, error: {} }", graph, e.getMessage());
            var errorStatus = Status.INVALID_ARGUMENT.withDescription(e.getMessage());
            responseObserver.onError(errorStatus.asException());
            return;
        }

        var op = Operation.create(
            request.getUserId(),
            "Execute graph of execution: executionId='%s'".formatted(request.getWorkflowId()),
            null,
            idempotencyKey,
            /* meta */ null);

        final GraphExecutionState graphExecution;
        try {
            graphExecution = queueManager.startGraph(request.getWorkflowId(), request.getWorkflowName(),
                request.getUserId(), graph, op);
        } catch (StatusException e) {
            LOG.error("Error while adding start graph event from workflow <{}>", request.getWorkflowId(), e);
            responseObserver.onError(e);
            return;
        } catch (Exception e) {
            if (idempotencyKey != null && handleIdempotencyKeyConflict(idempotencyKey, e, operationDao,
                responseObserver, GraphExecuteResponse.class, Duration.ofMillis(100), Duration.ofSeconds(5), LOG))
            {
                return;
            }

            LOG.error("Cannot create graph for workflow <" + request.getWorkflowId() + ">", e);

            var status = Status.INTERNAL.withDescription(e.getMessage());
            responseObserver.onError(status.asException());
            return;
        }

        var response = GraphExecuteResponse.newBuilder()
            .setStatus(graphExecution.toGrpc(schedulerApi))
            .build();
        var packed = Any.pack(response);

        try {
            withRetries(LOG, () -> operationDao.complete(op.id(), packed, null));
        } catch (Exception e) {
            LOG.error("Error while executing transaction: {}", e.getMessage(), e);

            try {
                queueManager.stopGraph(request.getWorkflowId(), graphExecution.id(),
                    "Cancel graph because of exception occurs in operation dao: " + e.getMessage());
            } catch (StatusException ex) {
                LOG.warn("Cannot stop graph because of error {} in operation dao: {}", e.getMessage(), ex.getMessage());
            }

            var errorStatus = Status.INTERNAL.withDescription("Error while execute graph: " + e.getMessage());
            try {
                operationDao.failOperation(op.id(), toProto(errorStatus), null, LOG);
            } catch (SQLException ex) {
                LOG.error("Cannot fail operation {}: {}", op.id(), ex.getMessage());
            }
            responseObserver.onError(errorStatus.asRuntimeException());
            return;
        }

        responseObserver.onNext(response);
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

        responseObserver.onNext(GraphStatusResponse.newBuilder()
            .setStatus(state.toGrpc(schedulerApi))
            .build());
        responseObserver.onCompleted();
    }

    @Override
    public void list(GraphListRequest request, StreamObserver<GraphListResponse> responseObserver) {
        final List<GraphExecutor.GraphExecutionStatus> graphs;

        try {
            graphs = dao.list(request.getWorkflowId())
                .stream()
                .map(t -> t.toGrpc(schedulerApi))
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
        responseObserver.onNext(GraphStopResponse.newBuilder()
            .setStatus(state.toGrpc(schedulerApi))
            .build());
        responseObserver.onCompleted();
    }

    private static boolean validateRequest(GraphExecuteRequest request, StreamObserver<GraphExecuteResponse> response) {
        if (request.getWorkflowId().isBlank()) {
            response.onError(Status.INVALID_ARGUMENT.withDescription("workflowId not set").asException());
            return false;
        }

        if (request.getWorkflowName().isBlank()) {
            response.onError(Status.INVALID_ARGUMENT.withDescription("workflowName not set").asException());
            return false;
        }

        if (request.getUserId().isBlank()) {
            response.onError(Status.INVALID_ARGUMENT.withDescription("userId not set").asException());
            return false;
        }

        if (request.getTasksCount() == 0) {
            response.onError(Status.INVALID_ARGUMENT.withDescription("tasks not set").asException());
            return false;
        }

        return true;
    }

    @PreDestroy
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

        final var internalUserOnly = new AllowInternalUserOnlyInterceptor(APP, iamChannel);

        server =
            newGrpcServer("0.0.0.0", config.getPort(),
                new AuthServerInterceptor(new AuthenticateServiceGrpcClient(APP, iamChannel)))
                .addService(ServerInterceptors.intercept(this, internalUserOnly))
                .build();

        server.start();

        LOG.info("Starting GraphExecutor service done");
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
                LOG.info("Stopping GraphExecutor service...");
                api.close();
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    LOG.error(e);
                }
                LOG.info("Stopping GraphExecutor service done");
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
