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
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static ai.lzy.channelmanager.grpc.ProtoConverter.makeCreateDirectChannelCommand;
import static ai.lzy.channelmanager.grpc.ProtoConverter.makeDestroyAllCommand;
import static ai.lzy.channelmanager.grpc.ProtoConverter.makeDestroyChannelCommand;
import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;

@SuppressWarnings({"UnstableApiUsage", "ResultOfMethodCallIgnored"})
public class ChannelManagerTest extends BaseTestWithIam {

    @Rule
    public PreparedDbRule iamDb = EmbeddedPostgresRules.preparedDatabase(ds -> {
    });
    @Rule
    public PreparedDbRule db = EmbeddedPostgresRules.preparedDatabase(ds -> {
    });

    private ApplicationContext channelManagerCtx;
    @SuppressWarnings("FieldCanBeLocal")
    private ChannelManagerConfig channelManagerConfig;
    private ChannelManager channelManagerApp;
    private ManagedChannel channel;

    private LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateBlockingStub unauthorizedChannelManagerPrivateClient;
    private LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateBlockingStub authorizedChannelManagerPrivateClient;

    @Before
    public void before() throws IOException, InterruptedException {
        super.setUp(DatabaseTestUtils.preparePostgresConfig("iam", iamDb.getConnectionInfo()));

        var props = DatabaseTestUtils.preparePostgresConfig("channel-manager", db.getConnectionInfo());
        channelManagerCtx = ApplicationContext.run(props);

        channelManagerConfig = channelManagerCtx.getBean(ChannelManagerConfig.class);
        channelManagerApp = new ChannelManager(channelManagerCtx);
        channelManagerApp.start();

        channel = newGrpcChannel(channelManagerConfig.getAddress(), LzyChannelManagerPrivateGrpc.SERVICE_NAME);
        unauthorizedChannelManagerPrivateClient = newBlockingClient(
            LzyChannelManagerPrivateGrpc.newBlockingStub(channel), "NoAuthTest", null);

        var internalUser = channelManagerConfig.getIam().createCredentials();
        authorizedChannelManagerPrivateClient = newBlockingClient(
            unauthorizedChannelManagerPrivateClient, "AuthTest", internalUser::token);
    }

    @After
    public void after() {
        channelManagerApp.stop();
        try {
            channelManagerApp.awaitTermination();
        } catch (InterruptedException ignored) {
            // ignored
        }

        channel.shutdown();
        try {
            channel.awaitTermination(60, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
            //ignored
        }

        DatabaseTestUtils.cleanup(channelManagerCtx.getBean(ChannelManagerDataSource.class));

        channelManagerCtx.close();
        super.after();
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
        final ChannelCreateResponse channelCreateResponse = authorizedChannelManagerPrivateClient.create(
            makeCreateDirectChannelCommand(UUID.randomUUID().toString(), "channel1"));
        final ChannelDestroyResponse channelDestroyResponse = authorizedChannelManagerPrivateClient.destroy(
            makeDestroyChannelCommand(channelCreateResponse.getChannelId()));
        try {
            authorizedChannelManagerPrivateClient.status(
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
            authorizedChannelManagerPrivateClient.status(
                ChannelStatusRequest.newBuilder().setChannelId(UUID.randomUUID().toString()).build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.NOT_FOUND.getCode(), e.getStatus().getCode());
        }
    }

    @Test
    public void testDestroyAll() {
        final String workflowId = UUID.randomUUID().toString();
        final ChannelCreateResponse channel1CreateResponse = authorizedChannelManagerPrivateClient.create(
            makeCreateDirectChannelCommand(workflowId, "channel1"));
        final ChannelCreateResponse channel2CreateResponse = authorizedChannelManagerPrivateClient.create(
            makeCreateDirectChannelCommand(UUID.randomUUID().toString(), "channel2"));
        authorizedChannelManagerPrivateClient.destroyAll(makeDestroyAllCommand(workflowId));

        try {
            authorizedChannelManagerPrivateClient.status(
                ChannelStatusRequest.newBuilder().setChannelId(channel1CreateResponse.getChannelId()).build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.NOT_FOUND.getCode(), e.getStatus().getCode());
        }
        final ChannelStatus status = authorizedChannelManagerPrivateClient.status(
            ChannelStatusRequest.newBuilder().setChannelId(channel2CreateResponse.getChannelId()).build());
        Assert.assertEquals(channel2CreateResponse.getChannelId(), status.getChannelId());
    }

    @Test
    public void testDestroyAllSameChannelName() {
        final String workflowId = UUID.randomUUID().toString();
        final ChannelCreateResponse channel1CreateResponse = authorizedChannelManagerPrivateClient.create(
            makeCreateDirectChannelCommand(workflowId, "channel1"));
        final ChannelCreateResponse channel2CreateResponse = authorizedChannelManagerPrivateClient.create(
            ProtoConverter.makeCreateDirectChannelCommand(UUID.randomUUID().toString(), "channel1"));
        authorizedChannelManagerPrivateClient.destroyAll(makeDestroyAllCommand(workflowId));

        try {
            authorizedChannelManagerPrivateClient.status(
                ChannelStatusRequest.newBuilder().setChannelId(channel1CreateResponse.getChannelId()).build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.NOT_FOUND.getCode(), e.getStatus().getCode());
        }
        final ChannelStatus status = authorizedChannelManagerPrivateClient.status(
            ChannelStatusRequest.newBuilder().setChannelId(channel2CreateResponse.getChannelId()).build());
        Assert.assertEquals(channel2CreateResponse.getChannelId(), status.getChannelId());
    }
}

