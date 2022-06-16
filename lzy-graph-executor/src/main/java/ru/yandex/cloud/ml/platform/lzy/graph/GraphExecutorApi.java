package ru.yandex.cloud.ml.platform.lzy.graph;

import static ru.yandex.cloud.ml.platform.lzy.model.utils.JwtCredentials.buildJWT;

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
import java.io.Reader;
import java.io.StringReader;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.graph.algo.GraphBuilder;
import ru.yandex.cloud.ml.platform.lzy.graph.algo.GraphBuilder.GraphValidationException;
import ru.yandex.cloud.ml.platform.lzy.graph.config.AuthConfig;
import ru.yandex.cloud.ml.platform.lzy.graph.config.ServiceConfig;
import ru.yandex.cloud.ml.platform.lzy.graph.db.DaoException;
import ru.yandex.cloud.ml.platform.lzy.graph.db.GraphExecutionDao;
import ru.yandex.cloud.ml.platform.lzy.graph.queue.QueueManager;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.credentials.Credentials;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.credentials.JwtCredentials;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.exceptions.AuthException;
import ru.yandex.cloud.ml.platform.lzy.iam.clients.AccessClient;
import ru.yandex.cloud.ml.platform.lzy.iam.clients.AuthenticateService;
import ru.yandex.cloud.ml.platform.lzy.iam.grpc.client.AccessServiceGrpcClient;
import ru.yandex.cloud.ml.platform.lzy.iam.grpc.client.AuthenticateServiceGrpcClient;
import ru.yandex.cloud.ml.platform.lzy.iam.grpc.context.AuthenticationContext;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.AuthPermission;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.impl.Workflow;
import ru.yandex.cloud.ml.platform.lzy.iam.utils.GrpcConfig;
import ru.yandex.cloud.ml.platform.lzy.model.logs.GrpcLogsInterceptor;
import ru.yandex.cloud.ml.platform.lzy.graph.model.GraphDescription;
import ru.yandex.cloud.ml.platform.lzy.graph.model.GraphExecutionState;
import ru.yandex.cloud.ml.platform.lzy.model.grpc.ChannelBuilder;
import yandex.cloud.lzy.v1.IAM;
import yandex.cloud.priv.datasphere.v2.lzy.GraphExecutorApi.*;
import yandex.cloud.priv.datasphere.v2.lzy.GraphExecutorGrpc;
import ru.yandex.cloud.ml.platform.lzy.iam.grpc.interceptors.AuthServerInterceptor;

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
    private final AuthConfig authConfig;
    private final QueueManager queueManager;
    private final AccessClient accessClient;

    private Server server;

    @Inject
    public GraphExecutorApi(
            GraphExecutionDao dao,
            ServiceConfig config,
            GraphBuilder graphBuilder,
            QueueManager queueManager,
            AuthConfig authConfig
    ) {
        this.dao = dao;
        this.config = config;
        this.graphBuilder = graphBuilder;
        this.queueManager = queueManager;
        this.authConfig = authConfig;
        accessClient = new AccessServiceGrpcClient(
            new GrpcConfig(authConfig.iamHost(), authConfig.iamPort()),
            this::credentialsSupplier);
    }

    @Override
    public void execute(GraphExecuteRequest request, StreamObserver<GraphExecuteResponse> responseObserver) {
        assertAuthorized(request.getWorkflowId(), AuthPermission.WORKFLOW_RUN);

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
        assertAuthorized(request.getWorkflowId(), AuthPermission.WORKFLOW_GET);

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
        assertAuthorized(request.getWorkflowId(), AuthPermission.WORKFLOW_GET);

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
        assertAuthorized(request.getWorkflowId(), AuthPermission.WORKFLOW_STOP);

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

        final AuthenticateService service = new AuthenticateServiceGrpcClient(
            new GrpcConfig(authConfig.iamHost(), authConfig.iamPort()));

        ServerBuilder<?> builder = NettyServerBuilder.forPort(config.port())
            .intercept(new AuthServerInterceptor(service))
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

    private Credentials credentialsSupplier() {
        try (final Reader reader = new StringReader(authConfig.privateKey())) {
            return new JwtCredentials(buildJWT(authConfig.serviceUid(), reader));
        } catch (IOException | NoSuchAlgorithmException | InvalidKeySpecException e) {
            throw new RuntimeException("Cannot build credentials");
        }
    }

    private void assertAuthorized(String workflowId, AuthPermission permission) {
        AuthenticationContext context = AuthenticationContext.current();

        if (context == null) {
            throw Status.UNAUTHENTICATED.asRuntimeException();
        }

        try {
            accessClient
                .withToken(context::getCredentials)
                .hasResourcePermission(context.getSubject(), new Workflow(workflowId), permission);

        } catch (AuthException e) {
            LOG.error("""
                <{}> trying to access workflow <{}> with permission <{}>, but unauthorized.
                Details: {}""",
                context.getSubject().id(), workflowId, permission, e.getInternalDetails()
            );
            throw e.toStatusRuntimeException();
        }
    }
}
