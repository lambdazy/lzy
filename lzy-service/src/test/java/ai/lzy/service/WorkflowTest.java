package ai.lzy.service;

import ai.lzy.allocator.test.BaseTestWithAllocator;
import ai.lzy.iam.grpc.interceptors.AuthServerInterceptor;
import ai.lzy.iam.resources.subjects.User;
import ai.lzy.iam.test.BaseTestWithIam;
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
import ai.lzy.v1.common.LMS3;
import ai.lzy.v1.portal.LzyPortalGrpc;
import ai.lzy.v1.workflow.LWFS;
import ai.lzy.v1.workflow.LzyWorkflowServiceGrpc;
import ai.lzy.service.config.LzyServiceConfig;
import com.google.common.net.HostAndPort;
import com.google.protobuf.Empty;
import io.grpc.*;
import io.grpc.netty.NettyServerBuilder;
import io.jsonwebtoken.Claims;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.PropertySource;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

public class WorkflowTest {
    private static final BaseTestWithIam iamTestContext = new BaseTestWithIam();
    private static final BaseTestWithAllocator allocatorTestContext = new BaseTestWithAllocator();

    @Rule
    public PreparedDbRule iamDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});
    @Rule
    public PreparedDbRule allocatorDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});
    @Rule
    public PreparedDbRule lzyServiceDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});

    private ApplicationContext context;
    private HostAndPort portalAddress;

    private MockS3Storage storageMock;
    private ChannelManagerMock channelManagerMock;

    private Server lzyServer;
    private Server storageServer;

    private ManagedChannel lzyServiceChannel;
    private LzyWorkflowServiceGrpc.LzyWorkflowServiceBlockingStub unauthorizedWorkflowClient;
    private LzyWorkflowServiceGrpc.LzyWorkflowServiceBlockingStub authorizedWorkflowClient;

    @Before
    public void setUp() throws IOException {
        var iamDbConfig = DatabaseTestUtils.preparePostgresConfig("iam", iamDb.getConnectionInfo());
        iamTestContext.setUp(iamDbConfig);

        var configOverrides = DatabaseTestUtils.preparePostgresConfig("allocator", allocatorDb.getConnectionInfo());
        configOverrides.put("allocator.thread-allocator.enabled", true);
        configOverrides.put("allocator.thread-allocator.vm-class-name", "ai.lzy.portal.App");
        allocatorTestContext.setUp(configOverrides);

        var lzyDbConfig = DatabaseTestUtils.preparePostgresConfig("lzy-service", lzyServiceDb.getConnectionInfo());
        context = ApplicationContext.run(PropertySource.of(lzyDbConfig));

        var config = context.getBean(LzyServiceConfig.class);
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

        lzyServer = NettyServerBuilder
            .forAddress(new InetSocketAddress(workflowAddress.getHost(), workflowAddress.getPort()))
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES)
            .addService(ServerInterceptors.intercept(context.getBean(LzyService.class), authInterceptor))
            .build();
        lzyServer.start();

        var internalUser = iam.createCredentials();
        lzyServiceChannel = ChannelBuilder.forAddress(workflowAddress).usePlaintext().build();
        unauthorizedWorkflowClient = LzyWorkflowServiceGrpc.newBlockingStub(lzyServiceChannel);
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
        lzyServiceChannel.shutdown();
        lzyServer.shutdown();
        lzyServer.awaitTermination();
        context.stop();
    }

    @Test
    public void testUnauthenticated() {
        var workflowName = "workflow_1";
        var executionId = "some-valid-id";

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
            }
        };

        thrown.forEach(e -> Assert.assertEquals(Status.PERMISSION_DENIED.getCode(), e.getStatus().getCode()));
    }

    @Test
    public void createWorkflow() {
        authorizedWorkflowClient.createWorkflow(
            LWFS.CreateWorkflowRequest.newBuilder().setWorkflowName("workflow_1").build());
        var thrown = Assert.assertThrows(StatusRuntimeException.class, () -> authorizedWorkflowClient
            .createWorkflow(LWFS.CreateWorkflowRequest.newBuilder().setWorkflowName("workflow_1").build()));

        var expectedStatusCode = Status.ALREADY_EXISTS.getCode();

        Assert.assertEquals(expectedStatusCode, thrown.getStatus().getCode());
    }

    @Test
    public void tempBucketCreationFailed() {
        storageServer.shutdown();

        var thrown = Assert.assertThrows(StatusRuntimeException.class, () ->
            authorizedWorkflowClient.createWorkflow(
                LWFS.CreateWorkflowRequest.newBuilder().setWorkflowName("workflow_1").build()));

        var expectedErrorCode = Status.UNAVAILABLE.getCode();

        Assert.assertEquals(expectedErrorCode, thrown.getStatus().getCode());
    }

    @Test
    public void createWorkflowFailedWithUserStorageMissedEndpoint() {
        var thrown = Assert.assertThrows(StatusRuntimeException.class, () ->
            authorizedWorkflowClient.createWorkflow(LWFS.CreateWorkflowRequest.newBuilder()
                .setWorkflowName("workflow_1")
                .setSnapshotStorage(LMS3.S3Locator.newBuilder()
                    .setKey("some-valid-key")
                    .setBucket("some-valid-bucket")
                    .build())
                .build()));

        var expectedErrorCode = Status.INVALID_ARGUMENT.getCode();

        Assert.assertEquals(expectedErrorCode, thrown.getStatus().getCode());
    }

    @Test
    public void tempBucketDeletedIfCreateExecutionFailed() {
        Assert.assertThrows(StatusRuntimeException.class, () -> {
            authorizedWorkflowClient.createWorkflow(
                LWFS.CreateWorkflowRequest.newBuilder().setWorkflowName("workflow_1").build());
            authorizedWorkflowClient.createWorkflow(
                LWFS.CreateWorkflowRequest.newBuilder().setWorkflowName("workflow_1").build());
        });

        TimeUtils.waitFlagUp(() -> storageMock.getBuckets().size() == 1, 300, TimeUnit.SECONDS);

        var expectedBucketCount = 1;
        var actualBucketCount = storageMock.getBuckets().size();

        Assert.assertEquals(expectedBucketCount, actualBucketCount);
    }

    @Test
    public void finishWorkflow() {
        var executionId = authorizedWorkflowClient.createWorkflow(
            LWFS.CreateWorkflowRequest.newBuilder().setWorkflowName("workflow_2").build()
        ).getExecutionId();

        authorizedWorkflowClient.finishWorkflow(
            LWFS.FinishWorkflowRequest.newBuilder()
                .setWorkflowName("workflow_2")
                .setExecutionId(executionId)
                .build());

        var thrownAlreadyFinished = Assert.assertThrows(StatusRuntimeException.class, () ->
            authorizedWorkflowClient.finishWorkflow(LWFS.FinishWorkflowRequest.newBuilder()
                .setWorkflowName("workflow_2")
                .setExecutionId(executionId)
                .build()));

        var thrownUnknownWorkflow = Assert.assertThrows(StatusRuntimeException.class, () ->
            authorizedWorkflowClient.finishWorkflow(
                LWFS.FinishWorkflowRequest.newBuilder()
                    .setWorkflowName("workflow_3")
                    .setExecutionId("execution_id")
                    .build()));

        Assert.assertEquals(Status.INTERNAL.getCode(), thrownAlreadyFinished.getStatus().getCode());
        Assert.assertEquals(Status.INTERNAL.getCode(), thrownUnknownWorkflow.getStatus().getCode());
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    public void testPortalStartedWhileCreatingWorkflow() {
        authorizedWorkflowClient.createWorkflow(
            LWFS.CreateWorkflowRequest.newBuilder().setWorkflowName("workflow_1").build());
        var portalChannel = ChannelBuilder.forAddress(portalAddress).usePlaintext().build();
        var portalClient = LzyPortalGrpc.newBlockingStub(portalChannel);
        portalClient.status(Empty.getDefaultInstance());
    }
}
