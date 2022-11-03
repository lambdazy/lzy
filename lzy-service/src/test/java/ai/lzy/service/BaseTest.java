package ai.lzy.service;

import ai.lzy.allocator.test.BaseTestWithAllocator;
import ai.lzy.channelmanager.grpc.ChannelManagerMock;
import ai.lzy.graph.test.GraphExecutorMock;
import ai.lzy.iam.grpc.interceptors.AuthServerInterceptor;
import ai.lzy.iam.resources.subjects.User;
import ai.lzy.iam.test.BaseTestWithIam;
import ai.lzy.portal.grpc.ProtoConverter;
import ai.lzy.service.config.LzyServiceConfig;
import ai.lzy.storage.test.BaseTestWithStorage;
import ai.lzy.util.auth.credentials.JwtUtils;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.util.auth.exceptions.AuthPermissionDeniedException;
import ai.lzy.util.auth.exceptions.AuthUnauthenticatedException;
import ai.lzy.util.grpc.*;
import ai.lzy.v1.common.LMS3;
import ai.lzy.v1.workflow.LWF;
import ai.lzy.v1.workflow.LWFS;
import ai.lzy.v1.workflow.LzyWorkflowServiceGrpc;
import ai.lzy.whiteboard.WhiteboardPrivateApiMock;
import com.google.common.net.HostAndPort;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyServerBuilder;
import io.jsonwebtoken.Claims;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.PropertySource;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import org.junit.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static ai.lzy.model.db.test.DatabaseTestUtils.preparePostgresConfig;
import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;

public class BaseTest {
    private static final BaseTestWithIam iamTestContext = new BaseTestWithIam();
    private static final BaseTestWithStorage storageTestContext = new BaseTestWithStorage();
    private static final BaseTestWithAllocator allocatorTestContext = new BaseTestWithAllocator();

    @Rule
    public PreparedDbRule iamDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});
    @Rule
    public PreparedDbRule storageDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});
    @Rule
    public PreparedDbRule allocatorDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});
    @Rule
    public PreparedDbRule lzyServiceDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});

    protected ApplicationContext context;
    protected LzyServiceConfig config;

    protected GraphExecutorMock graphExecutorMock;

    private Server channelManagerServer;

    private Server whiteboardServer;

    private Server lzyServer;

    private ManagedChannel lzyServiceChannel;
    private LzyWorkflowServiceGrpc.LzyWorkflowServiceBlockingStub unauthorizedWorkflowClient;
    protected LzyWorkflowServiceGrpc.LzyWorkflowServiceBlockingStub authorizedWorkflowClient;

    protected RenewableJwt internalUserCredentials;
    protected AuthServerInterceptor authInterceptor;

    @Before
    public void setUp() throws IOException, InterruptedException {
        var iamDbConfig = preparePostgresConfig("iam", iamDb.getConnectionInfo());
        iamTestContext.setUp(iamDbConfig);

        var storageDbConfig = preparePostgresConfig("storage", storageDb.getConnectionInfo());
        storageTestContext.setUp(storageDbConfig);

        var allocatorConfigOverrides = preparePostgresConfig("allocator", allocatorDb.getConnectionInfo());
        allocatorConfigOverrides.put("allocator.thread-allocator.enabled", true);
        allocatorConfigOverrides.put("allocator.thread-allocator.vm-class-name", "ai.lzy.portal.App");
        allocatorTestContext.setUp(allocatorConfigOverrides);

        var lzyDbConfig = preparePostgresConfig("lzy-service", lzyServiceDb.getConnectionInfo());
        context = ApplicationContext.run(PropertySource.of(lzyDbConfig));

        config = context.getBean(LzyServiceConfig.class);

        authInterceptor = new AuthServerInterceptor(credentials -> {
            var iam = config.getIam();

            var user = Objects.requireNonNull(JwtUtils.parseJwt(credentials.token())).get(Claims.ISSUER);

            if (user == null) {
                throw new AuthUnauthenticatedException("heck");
            }
            if (!iam.getInternalUserName().equals(user)) {
                throw new AuthPermissionDeniedException("heck");
            }

            return new User(iam.getInternalUserName());
        });

        var workflowAddress = HostAndPort.fromString(config.getAddress());

        var channelManagerAddress = HostAndPort.fromString(config.getChannelManagerAddress());
        var channelManagerMock = new ChannelManagerMock();
        channelManagerServer = GrpcUtils.newGrpcServer(channelManagerAddress, GrpcUtils.NO_AUTH)
            .addService(channelManagerMock.publicService)
            .addService(channelManagerMock.privateService)
            .build();
        channelManagerServer.start();

        lzyServer = App.createServer(workflowAddress, authInterceptor, context.getBean(LzyService.class));
        lzyServer.start();

        var whiteboardAddress = HostAndPort.fromString(config.getWhiteboardAddress());
        whiteboardServer = NettyServerBuilder
            .forAddress(new InetSocketAddress(whiteboardAddress.getHost(), whiteboardAddress.getPort()))
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES)
            .intercept(authInterceptor)
            .intercept(GrpcLogsInterceptor.server())
            .intercept(RequestIdInterceptor.server(true))
            .intercept(GrpcHeadersServerInterceptor.create())
            .addService(new WhiteboardPrivateApiMock())
            .build();
        whiteboardServer.start();

        lzyServiceChannel = newGrpcChannel(workflowAddress, LzyWorkflowServiceGrpc.SERVICE_NAME);
        unauthorizedWorkflowClient = LzyWorkflowServiceGrpc.newBlockingStub(lzyServiceChannel);

        internalUserCredentials = config.getIam().createRenewableToken();
        authorizedWorkflowClient = newBlockingClient(unauthorizedWorkflowClient, "TestClient",
            () -> internalUserCredentials.get().token());
    }

    @After
    public void tearDown() throws SQLException, InterruptedException {
        iamTestContext.after();
        allocatorTestContext.after();
        storageTestContext.after();
        channelManagerServer.shutdown();
        channelManagerServer.awaitTermination();
        lzyServiceChannel.shutdown();
        lzyServer.shutdown();
        lzyServer.awaitTermination();
        whiteboardServer.shutdown();
        whiteboardServer.awaitTermination();
        context.stop();
    }

    protected void shutdownStorage() throws SQLException, InterruptedException {
        storageTestContext.after();
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    public void testUnauthenticated() {
        var workflowName = "workflow_1";
        var executionId = "some-valid-execution-id";
        var graphName = "simple-graph";
        var graphId = "some-valid-graph-id";

        var thrown = new ArrayList<StatusRuntimeException>() {
            {
                add(Assert.assertThrows(StatusRuntimeException.class, () -> unauthorizedWorkflowClient.createWorkflow(
                    LWFS.CreateWorkflowRequest.newBuilder().setWorkflowName(workflowName).build())));

                add(Assert.assertThrows(StatusRuntimeException.class, () -> unauthorizedWorkflowClient.deleteWorkflow(
                    LWFS.DeleteWorkflowRequest.newBuilder().setWorkflowName(workflowName).build())));

                add(Assert.assertThrows(StatusRuntimeException.class, () -> unauthorizedWorkflowClient.attachWorkflow(
                    LWFS.AttachWorkflowRequest.newBuilder()
                        .setWorkflowName(workflowName)
                        .setExecutionId(executionId)
                        .build())));

                add(Assert.assertThrows(StatusRuntimeException.class, () -> unauthorizedWorkflowClient.finishWorkflow(
                    LWFS.FinishWorkflowRequest.newBuilder()
                        .setWorkflowName(workflowName)
                        .setExecutionId(executionId)
                        .setReason("my will").build())));

                add(Assert.assertThrows(StatusRuntimeException.class, () -> unauthorizedWorkflowClient.executeGraph(
                    LWFS.ExecuteGraphRequest.newBuilder()
                        .setExecutionId(executionId)
                        .setGraph(LWF.Graph.newBuilder()
                            .setName(graphName)
                            .build())
                        .build()
                )));

                add(Assert.assertThrows(StatusRuntimeException.class, () -> unauthorizedWorkflowClient.graphStatus(
                    LWFS.GraphStatusRequest.newBuilder()
                        .setExecutionId(executionId)
                        .setGraphId(graphId)
                        .build()
                )));

                add(Assert.assertThrows(StatusRuntimeException.class, () -> unauthorizedWorkflowClient.stopGraph(
                    LWFS.StopGraphRequest.newBuilder()
                        .setExecutionId(executionId)
                        .setGraphId(graphId)
                        .build()
                )));
            }
        };

        thrown.forEach(e -> Assert.assertEquals(Status.UNAUTHENTICATED.getCode(), e.getStatus().getCode()));
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    public void testPermissionDenied() {
        var workflowName = "workflow_1";
        var executionId = "some-valid-execution-id";
        var graphName = "simple-graph";
        var graphId = "some-valid-graph-id";
        var client = unauthorizedWorkflowClient.withInterceptors(ClientHeaderInterceptor.header(
            GrpcHeaders.AUTHORIZATION, JwtUtils.invalidCredentials("user", "GITHUB")::token));

        var thrown = new ArrayList<StatusRuntimeException>() {
            {
                add(Assert.assertThrows(StatusRuntimeException.class, () -> client.createWorkflow(
                    LWFS.CreateWorkflowRequest.newBuilder().setWorkflowName(workflowName).build())));

                add(Assert.assertThrows(StatusRuntimeException.class, () -> client.deleteWorkflow(
                    LWFS.DeleteWorkflowRequest.newBuilder().setWorkflowName(workflowName).build())));

                add(Assert.assertThrows(StatusRuntimeException.class, () -> client.attachWorkflow(
                    LWFS.AttachWorkflowRequest.newBuilder()
                        .setWorkflowName(workflowName)
                        .setExecutionId(executionId)
                        .build())));

                add(Assert.assertThrows(StatusRuntimeException.class, () -> client.finishWorkflow(
                    LWFS.FinishWorkflowRequest.newBuilder()
                        .setWorkflowName(workflowName)
                        .setExecutionId(executionId)
                        .setReason("my will")
                        .build())));

                add(Assert.assertThrows(StatusRuntimeException.class, () -> client.executeGraph(
                    LWFS.ExecuteGraphRequest.newBuilder()
                        .setExecutionId(executionId)
                        .setGraph(LWF.Graph.newBuilder()
                            .setName(graphName)
                            .build())
                        .build()
                )));

                add(Assert.assertThrows(StatusRuntimeException.class, () -> client.graphStatus(
                    LWFS.GraphStatusRequest.newBuilder()
                        .setExecutionId(executionId)
                        .setGraphId(graphId)
                        .build()
                )));

                add(Assert.assertThrows(StatusRuntimeException.class, () -> client.stopGraph(
                    LWFS.StopGraphRequest.newBuilder()
                        .setExecutionId(executionId)
                        .setGraphId(graphId)
                        .build()
                )));
            }
        };

        thrown.forEach(e -> Assert.assertEquals(Status.PERMISSION_DENIED.getCode(), e.getStatus().getCode()));
    }

    public static String buildSlotUri(String key, LMS3.S3Locator locator) {
        return ProtoConverter.getSlotUri(LMS3.S3Locator.newBuilder()
            .setKey(key)
            .setBucket(locator.getBucket())
            .setAmazon(locator.getAmazon())
            .setAzure(locator.getAzure())
            .build());
    }
}
