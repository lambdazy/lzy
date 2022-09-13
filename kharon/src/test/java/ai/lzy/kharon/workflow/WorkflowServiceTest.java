package ai.lzy.kharon.workflow;

import ai.lzy.allocator.test.BaseTestWithAllocator;
import ai.lzy.iam.grpc.interceptors.AuthServerInterceptor;
import ai.lzy.iam.resources.subjects.User;
import ai.lzy.iam.test.BaseTestWithIam;
import ai.lzy.kharon.KharonConfig;
import ai.lzy.model.db.test.DatabaseTestUtils;
import ai.lzy.storage.impl.MockS3Storage;
import ai.lzy.test.TimeUtils;
import ai.lzy.test.mocks.ChannelManagerMock;
import ai.lzy.util.auth.credentials.JwtUtils;
import ai.lzy.util.auth.exceptions.AuthPermissionDeniedException;
import ai.lzy.util.auth.exceptions.AuthUnauthenticatedException;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.util.grpc.ClientHeaderInterceptor;
import ai.lzy.util.grpc.GrpcHeaders;
import ai.lzy.v1.LzyPortalGrpc;
import ai.lzy.v1.workflow.LWS;
import ai.lzy.v1.workflow.LWS.CreateWorkflowRequest;
import ai.lzy.v1.workflow.LWS.FinishWorkflowRequest;
import ai.lzy.v1.workflow.LzyWorkflowServiceGrpc;
import ai.lzy.v1.workflow.LzyWorkflowServiceGrpc.LzyWorkflowServiceBlockingStub;
import com.google.common.net.HostAndPort;
import com.google.protobuf.Empty;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
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
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

@SuppressWarnings({"ResultOfMethodCallIgnored", "UnstableApiUsage"})
public class WorkflowServiceTest {
    private static final BaseTestWithIam iamTestContext = new BaseTestWithIam();
    private static final BaseTestWithAllocator allocatorTestContext = new BaseTestWithAllocator();

    @Rule
    public PreparedDbRule iamDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});
    @Rule
    public PreparedDbRule kharonDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});
    @Rule
    public PreparedDbRule allocatorDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});

    private ApplicationContext context;
    private HostAndPort portalAddress;

    private MockS3Storage storageMock;
    private ChannelManagerMock channelManagerMock;

    private Server storageServer;
    private Server workflowServer;

    private LzyWorkflowServiceBlockingStub unauthorizedWorkflowClient;
    private LzyWorkflowServiceBlockingStub authorizedWorkflowClient;

    @Before
    public void setUp() throws IOException, NoSuchAlgorithmException, InvalidKeySpecException {
        var iamDbConfig = DatabaseTestUtils.preparePostgresConfig("iam", iamDb.getConnectionInfo());
        iamTestContext.setUp(iamDbConfig);

        var configOverrides = DatabaseTestUtils.preparePostgresConfig("allocator", allocatorDb.getConnectionInfo());
        configOverrides.put("allocator.thread-allocator.enabled", true);
        configOverrides.put("allocator.thread-allocator.vm-class-name", "ai.lzy.portal.App");
        allocatorTestContext.setUp(configOverrides);

        var kharonDbConfig = DatabaseTestUtils.preparePostgresConfig("kharon", kharonDb.getConnectionInfo());
        context = ApplicationContext.run(PropertySource.of(kharonDbConfig), "test");

        var config = context.getBean(KharonConfig.class);
        var workflowAddress = HostAndPort.fromString(config.getAddress());
        var storageAddress = HostAndPort.fromString(config.getStorage().getAddress());
        portalAddress = HostAndPort.fromParts("localhost", config.getPortal().getPortalApiPort());

        var iam = config.getIam();
        var authInterceptor = new AuthServerInterceptor(credentials -> {
            var user = JwtUtils.parseJwt(credentials.token()).get(Claims.ISSUER);

            if (user == null) {
                throw new AuthUnauthenticatedException("heck");
            }
            if (!iam.getInternalUserName().equals(user)) {
                throw new AuthPermissionDeniedException("heck");
            }

            return new User(iam.getInternalUserName());
        });

        storageMock = new MockS3Storage();
        storageServer = NettyServerBuilder
            .forAddress(new InetSocketAddress(storageAddress.getHost(), storageAddress.getPort()))
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES)
            .addService(ServerInterceptors.intercept(storageMock, authInterceptor))
            .build();
        storageServer.start();

        channelManagerMock = new ChannelManagerMock(HostAndPort.fromString(config.getChannelManagerAddress()));
        channelManagerMock.start();

        workflowServer = NettyServerBuilder
            .forAddress(new InetSocketAddress(workflowAddress.getHost(), workflowAddress.getPort()))
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES)
            .addService(ServerInterceptors.intercept(context.getBean(WorkflowService.class), authInterceptor))
            .build();
        workflowServer.start();

        var channel = ChannelBuilder.forAddress(workflowAddress).usePlaintext().build();
        var internalUser = iam.createCredentials();
        unauthorizedWorkflowClient = LzyWorkflowServiceGrpc.newBlockingStub(channel);
        authorizedWorkflowClient = unauthorizedWorkflowClient.withInterceptors(
            ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION, internalUser::token));
    }

    @After
    public void tearDown() throws SQLException, InterruptedException {
        iamTestContext.after();
        allocatorTestContext.after();
        storageServer.shutdown();
        storageServer.awaitTermination();
        channelManagerMock.stop();
        workflowServer.shutdown();
        workflowServer.awaitTermination();
        context.stop();
    }

    @Test
    public void testUnauthenticated() {
        var workflowName = "workflow_1";
        var executionId = "some-valid-id";

        var thrown = new ArrayList<StatusRuntimeException>() {
            {
                add(Assert.assertThrows(StatusRuntimeException.class, () -> unauthorizedWorkflowClient.createWorkflow(
                    CreateWorkflowRequest.newBuilder().setWorkflowName(workflowName).build())));

                add(Assert.assertThrows(StatusRuntimeException.class, () -> unauthorizedWorkflowClient.deleteWorkflow(
                    LWS.DeleteWorkflowRequest.newBuilder().setWorkflowName(workflowName).build())));

                add(Assert.assertThrows(StatusRuntimeException.class, () -> unauthorizedWorkflowClient.attachWorkflow(
                    LWS.AttachWorkflowRequest.newBuilder()
                        .setWorkflowName(workflowName)
                        .setExecutionId(executionId)
                        .build())));

                add(Assert.assertThrows(StatusRuntimeException.class, () -> unauthorizedWorkflowClient.finishWorkflow(
                    FinishWorkflowRequest.newBuilder()
                        .setWorkflowName(workflowName)
                        .setExecutionId(executionId)
                        .setReason("my will").build())));
            }
        };

        thrown.forEach(e -> Assert.assertEquals(Status.UNAUTHENTICATED.getCode(), e.getStatus().getCode()));
    }

    @Test
    public void testPermissionDenied() {
        var workflowName = "workflow_1";
        var executionId = "some-valid-id";
        var client = unauthorizedWorkflowClient.withInterceptors(ClientHeaderInterceptor.header(
            GrpcHeaders.AUTHORIZATION, JwtUtils.invalidCredentials("user", "GITHUB")::token));

        var thrown = new ArrayList<StatusRuntimeException>() {
            {
                add(Assert.assertThrows(StatusRuntimeException.class, () -> client.createWorkflow(
                    CreateWorkflowRequest.newBuilder().setWorkflowName(workflowName).build())));

                add(Assert.assertThrows(StatusRuntimeException.class, () -> client.deleteWorkflow(
                    LWS.DeleteWorkflowRequest.newBuilder().setWorkflowName(workflowName).build())));

                add(Assert.assertThrows(StatusRuntimeException.class, () -> client.attachWorkflow(
                    LWS.AttachWorkflowRequest.newBuilder()
                        .setWorkflowName(workflowName)
                        .setExecutionId(executionId)
                        .build())));

                add(Assert.assertThrows(StatusRuntimeException.class, () -> client.finishWorkflow(
                    FinishWorkflowRequest.newBuilder()
                        .setWorkflowName(workflowName)
                        .setExecutionId(executionId)
                        .setReason("my will")
                        .build())));
            }
        };

        thrown.forEach(e -> Assert.assertEquals(Status.PERMISSION_DENIED.getCode(), e.getStatus().getCode()));
    }

    @Test
    public void createWorkflow() {
        authorizedWorkflowClient.createWorkflow(
            CreateWorkflowRequest.newBuilder().setWorkflowName("workflow_1").build());

        try {
            authorizedWorkflowClient.createWorkflow(
                CreateWorkflowRequest.newBuilder().setWorkflowName("workflow_1").build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            if (!Status.ALREADY_EXISTS.getCode().equals(e.getStatus().getCode())) {
                e.printStackTrace(System.err);
                Assert.fail(e.getMessage());
            }
        }
    }

    @Test
    public void tempBucketCreationFailed() {
        storageServer.shutdown();

        var thrown = Assert.assertThrows(StatusRuntimeException.class, () ->
            authorizedWorkflowClient.createWorkflow(
                CreateWorkflowRequest.newBuilder().setWorkflowName("workflow_1").build()));

        var expectedErrorCode = Status.UNAVAILABLE.getCode();
        var expectedErrorMessage = "Cannot create internal storage";

        Assert.assertEquals(expectedErrorCode, thrown.getStatus().getCode());
        Assert.assertEquals(expectedErrorMessage, thrown.getStatus().getDescription());
    }

    @Test
    public void tempBucketDeletedIfCreateExecutionFailed() {
        Assert.assertThrows(StatusRuntimeException.class, () -> {
            authorizedWorkflowClient.createWorkflow(
                CreateWorkflowRequest.newBuilder().setWorkflowName("workflow_1").build());
            authorizedWorkflowClient.createWorkflow(
                CreateWorkflowRequest.newBuilder().setWorkflowName("workflow_1").build());
        });

        TimeUtils.waitFlagUp(() -> storageMock.getBuckets().size() == 1, 300, TimeUnit.SECONDS);

        var expectedBucketCount = 1;
        var actualBucketCount = storageMock.getBuckets().size();

        Assert.assertEquals(expectedBucketCount, actualBucketCount);
    }

    @Test
    public void finishWorkflow() {
        var executionId = authorizedWorkflowClient.createWorkflow(
            CreateWorkflowRequest.newBuilder().setWorkflowName("workflow_2").build()
        ).getExecutionId();

        authorizedWorkflowClient.finishWorkflow(
            FinishWorkflowRequest.newBuilder()
                .setWorkflowName("workflow_2")
                .setExecutionId(executionId)
                .build());

        var thrownAlreadyFinished = Assert.assertThrows(StatusRuntimeException.class, () ->
            authorizedWorkflowClient.finishWorkflow(FinishWorkflowRequest.newBuilder()
                .setWorkflowName("workflow_2")
                .setExecutionId(executionId)
                .build()));

        var thrownUnknownWorkflow = Assert.assertThrows(StatusRuntimeException.class, () ->
            authorizedWorkflowClient.finishWorkflow(
                FinishWorkflowRequest.newBuilder()
                    .setWorkflowName("workflow_3")
                    .setExecutionId("execution_id")
                    .build()));

        Assert.assertEquals(Status.INTERNAL.getCode(), thrownAlreadyFinished.getStatus().getCode());
        Assert.assertEquals(Status.INTERNAL.getCode(), thrownUnknownWorkflow.getStatus().getCode());

        Assert.assertEquals("Already finished", thrownAlreadyFinished.getStatus().getDescription());
        Assert.assertEquals("Unknown workflow execution", thrownUnknownWorkflow.getStatus().getDescription());
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    public void testPortalStartedWhileCreatingWorkflow() {
        authorizedWorkflowClient.createWorkflow(
            LWS.CreateWorkflowRequest.newBuilder().setWorkflowName("workflow_1").build());
        var portalChannel = ChannelBuilder.forAddress(portalAddress).usePlaintext().build();
        var portalClient = LzyPortalGrpc.newBlockingStub(portalChannel);
        portalClient.status(Empty.getDefaultInstance());
    }
}
