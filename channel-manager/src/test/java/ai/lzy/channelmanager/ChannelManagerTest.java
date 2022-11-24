package ai.lzy.channelmanager;

import ai.lzy.channelmanager.db.ChannelManagerDataSource;
import ai.lzy.channelmanager.grpc.ProtoConverter;
import ai.lzy.iam.test.BaseTestWithIam;
import ai.lzy.model.db.test.DatabaseTestUtils;
import ai.lzy.v1.channel.LCMPS.ChannelCreateResponse;
import ai.lzy.v1.channel.LCMPS.ChannelDestroyResponse;
import ai.lzy.v1.channel.LCMPS.ChannelStatus;
import ai.lzy.v1.channel.LCMPS.ChannelStatusRequest;
import ai.lzy.v1.channel.LzyChannelManagerPrivateGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.micronaut.context.ApplicationContext;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static ai.lzy.channelmanager.grpc.ProtoConverter.makeCreateDirectChannelCommand;
import static ai.lzy.channelmanager.grpc.ProtoConverter.makeDestroyAllCommand;
import static ai.lzy.channelmanager.grpc.ProtoConverter.makeDestroyChannelCommand;
import static ai.lzy.model.db.test.DatabaseTestUtils.preparePostgresConfig;
import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;

public class ChannelManagerTest {
    private static final BaseTestWithIam iamTestContext = new BaseTestWithIam();

    @Rule
    public PreparedDbRule iamDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});
    @Rule
    public PreparedDbRule channelManagerDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});

    private ApplicationContext context;
    private ChannelManagerConfig config;
    private ChannelManagerApp app;
    private ManagedChannel channel;

    private LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateBlockingStub unauthorizedPrivateClient;
    private LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateBlockingStub authorizedPrivateClient;

    @Before
    public void before() throws IOException, InterruptedException {
        var iamDbConfig = preparePostgresConfig("iam", iamDb.getConnectionInfo());
        iamTestContext.setUp(iamDbConfig);

        var channelManagerDbConfig = preparePostgresConfig("channel-manager", channelManagerDb.getConnectionInfo());
        context = ApplicationContext.run(channelManagerDbConfig);
        app = context.getBean(ChannelManagerApp.class);
        app.start();

        config = context.getBean(ChannelManagerConfig.class);
        channel = newGrpcChannel(config.getAddress(), LzyChannelManagerPrivateGrpc.SERVICE_NAME);
        unauthorizedPrivateClient = newBlockingClient(
            LzyChannelManagerPrivateGrpc.newBlockingStub(channel), "NoAuthTest", null);

        var internalUserCredentials = config.getIam().createRenewableToken();
        authorizedPrivateClient = newBlockingClient(
            unauthorizedPrivateClient, "AuthTest", () -> internalUserCredentials.get().token());
    }

    @After
    public void after() throws SQLException, InterruptedException {
        iamTestContext.after();
        app.stop();
        app.awaitTermination();
        channel.shutdown();
        channel.awaitTermination(60, TimeUnit.SECONDS);
        DatabaseTestUtils.cleanup(context.getBean(ChannelManagerDataSource.class));
        context.close();
    }

    /*
    @Test
    public void testUnauthenticated() {
        try {
            unauthorizedChannelManagerPrivateClient.create(ChannelCreateRequest.newBuilder().build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.UNAUTHENTICATED.getCode(), e.getStatus().getCode());
        }

        try {
            unauthorizedChannelManagerPrivateClient.destroy(ChannelDestroyRequest.newBuilder().build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.UNAUTHENTICATED.getCode(), e.getStatus().getCode());
        }

        try {
            unauthorizedChannelManagerPrivateClient.destroyAll(ChannelDestroyAllRequest.newBuilder().build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.UNAUTHENTICATED.getCode(), e.getStatus().getCode());
        }

        try {
            unauthorizedChannelManagerPrivateClient.status(ChannelStatusRequest.newBuilder().build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.UNAUTHENTICATED.getCode(), e.getStatus().getCode());
        }

        try {
            unauthorizedChannelManagerPrivateClient.statusAll(ChannelStatusAllRequest.newBuilder().build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.UNAUTHENTICATED.getCode(), e.getStatus().getCode());
        }

        try {
            unauthorizedChannelManagerPrivateClient.bind(BindRequest.newBuilder().build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.UNAUTHENTICATED.getCode(), e.getStatus().getCode());
        }

        try {
            unauthorizedChannelManagerPrivateClient.unbind(SlotDetach.newBuilder().build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.UNAUTHENTICATED.getCode(), e.getStatus().getCode());
        }
    }

    @Test
    public void testCreateSuccess() {
        final ChannelCreateResponse channelCreateResponse = authorizedChannelManagerPrivateClient.create(
            GrpcUtils.makeCreateDirectChannelCommand(UUID.randomUUID().toString(), "channel1"));
        Assert.assertNotNull(channelCreateResponse.getChannelId());
        Assert.assertTrue(channelCreateResponse.getChannelId().length() > 1);
    }

    @Test
    public void testCreateEmptyWorkflow() {
        try {
            authorizedChannelManagerPrivateClient.create(ChannelCreateRequest.newBuilder()
                    .setChannelSpec(ChannelSpec.newBuilder()
                        .setChannelName("channel1")
                        .setDirect(DirectChannelType.newBuilder().build())
                        .setContentType(LMD.DataScheme.newBuilder()
                            .setSchemeContent("text")
                            .setDataFormat("plain")
                            .build())
                        .build())
                    .build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.INVALID_ARGUMENT.getCode(), e.getStatus().getCode());
        }
    }

    @Test
    public void testCreateEmptyChannelSpec() {
        try {
            authorizedChannelManagerPrivateClient.create(
                ChannelCreateRequest.newBuilder()
                    .setWorkflowId(UUID.randomUUID().toString())
                    .build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.INVALID_ARGUMENT.getCode(), e.getStatus().getCode());
        }
    }

    @Test
    public void testCreateEmptySpecName() {
        try {
            authorizedChannelManagerPrivateClient.create(
                ChannelCreateRequest.newBuilder()
                    .setWorkflowId(UUID.randomUUID().toString())
                    .setChannelSpec(ChannelSpec.newBuilder()
                        .setDirect(DirectChannelType.newBuilder().build())
                        .setContentType(LMD.DataScheme.newBuilder()
                            .setSchemeContent("text")
                            .setDataFormat("plain")
                            .build())
                        .build())
                    .build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.INVALID_ARGUMENT.getCode(), e.getStatus().getCode());
        }
    }

    @Test
    public void testCreateEmptySpecType() {
        try {
            authorizedChannelManagerPrivateClient.create(
                ChannelCreateRequest.newBuilder()
                    .setWorkflowId(UUID.randomUUID().toString())
                    .setChannelSpec(ChannelSpec.newBuilder()
                        .setChannelName("channel1")
                        .setContentType(LMD.DataScheme.newBuilder()
                            .setSchemeContent("text")
                            .setDataFormat("plain")
                            .build())
                        .build())
                    .build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.INVALID_ARGUMENT.getCode(), e.getStatus().getCode());
        }
    }

    @Test
    public void testCreateEmptySpecContentType() {
        try {
            authorizedChannelManagerPrivateClient.create(
                ChannelCreateRequest.newBuilder()
                    .setWorkflowId(UUID.randomUUID().toString())
                    .setChannelSpec(ChannelSpec.newBuilder().setChannelName("channel1").setDirect(
                            DirectChannelType.newBuilder().build())
                        .build())
                    .build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.INVALID_ARGUMENT.getCode(), e.getStatus().getCode());
        }
    }

    @Test
    public void testCreateEmptySpecContentTypeType() {
        try {
            authorizedChannelManagerPrivateClient.create(
                ChannelCreateRequest.newBuilder()
                    .setWorkflowId(UUID.randomUUID().toString())
                    .setChannelSpec(ChannelSpec.newBuilder()
                        .setChannelName("channel1")
                        .setDirect(DirectChannelType.newBuilder().build())
                        .setContentType(LMD.DataScheme.newBuilder().setDataFormat("plain").build())
                        .build())
                    .build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.INVALID_ARGUMENT.getCode(), e.getStatus().getCode());
        }
    }

    @Test
    public void testCreateEmptySpecContentTypeSchemeType() {
        try {
            authorizedChannelManagerPrivateClient.create(
                ChannelCreateRequest.newBuilder()
                    .setWorkflowId(UUID.randomUUID().toString())
                    .setChannelSpec(ChannelSpec.newBuilder()
                        .setChannelName("channel1")
                        .setDirect(DirectChannelType.newBuilder().build())
                        .setContentType(LMD.DataScheme.newBuilder().setSchemeContent("text"))
                        .build())
                    .build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.INVALID_ARGUMENT.getCode(), e.getStatus().getCode());
        }
    }
    */

    @Test
    public void testCreateAndDestroy() {
        final ChannelCreateResponse channelCreateResponse = authorizedPrivateClient.create(
            makeCreateDirectChannelCommand(UUID.randomUUID().toString(), "channel1"));
        final ChannelDestroyResponse channelDestroyResponse = authorizedPrivateClient.destroy(
            makeDestroyChannelCommand(channelCreateResponse.getChannelId()));
        try {
            authorizedPrivateClient.status(
                ChannelStatusRequest.newBuilder().setChannelId(channelCreateResponse.getChannelId()).build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.NOT_FOUND.getCode(), e.getStatus().getCode());
        }

        Assert.assertNotNull(channelCreateResponse.getChannelId());
        Assert.assertTrue(channelCreateResponse.getChannelId().length() > 1);
        Assert.assertNotNull(channelDestroyResponse);
    }

    @Test
    public void testDestroyNonexistentChannel() {
        try {
            authorizedPrivateClient.status(
                ChannelStatusRequest.newBuilder().setChannelId(UUID.randomUUID().toString()).build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.NOT_FOUND.getCode(), e.getStatus().getCode());
        }
    }

    @Test
    public void testDestroyAll() {
        final String workflowId = UUID.randomUUID().toString();
        final ChannelCreateResponse channel1CreateResponse = authorizedPrivateClient.create(
            makeCreateDirectChannelCommand(workflowId, "channel1"));
        final ChannelCreateResponse channel2CreateResponse = authorizedPrivateClient.create(
            makeCreateDirectChannelCommand(UUID.randomUUID().toString(), "channel2"));
        authorizedPrivateClient.destroyAll(makeDestroyAllCommand(workflowId));

        try {
            authorizedPrivateClient.status(
                ChannelStatusRequest.newBuilder().setChannelId(channel1CreateResponse.getChannelId()).build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.NOT_FOUND.getCode(), e.getStatus().getCode());
        }
        final ChannelStatus status = authorizedPrivateClient.status(
            ChannelStatusRequest.newBuilder().setChannelId(channel2CreateResponse.getChannelId()).build());
        Assert.assertEquals(channel2CreateResponse.getChannelId(), status.getChannelId());
    }

    @Test
    public void testDestroyAllSameChannelName() {
        final String workflowId = UUID.randomUUID().toString();
        final ChannelCreateResponse channel1CreateResponse = authorizedPrivateClient.create(
            makeCreateDirectChannelCommand(workflowId, "channel1"));
        final ChannelCreateResponse channel2CreateResponse = authorizedPrivateClient.create(
            ProtoConverter.makeCreateDirectChannelCommand(UUID.randomUUID().toString(), "channel1"));
        authorizedPrivateClient.destroyAll(makeDestroyAllCommand(workflowId));

        try {
            authorizedPrivateClient.status(
                ChannelStatusRequest.newBuilder().setChannelId(channel1CreateResponse.getChannelId()).build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.NOT_FOUND.getCode(), e.getStatus().getCode());
        }
        final ChannelStatus status = authorizedPrivateClient.status(
            ChannelStatusRequest.newBuilder().setChannelId(channel2CreateResponse.getChannelId()).build());
        Assert.assertEquals(channel2CreateResponse.getChannelId(), status.getChannelId());
    }
}

