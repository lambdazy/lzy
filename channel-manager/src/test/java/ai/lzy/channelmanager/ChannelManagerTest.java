package ai.lzy.channelmanager;

import ai.lzy.channelmanager.db.ChannelManagerDataSource;
import ai.lzy.iam.test.BaseTestWithIam;
import ai.lzy.model.db.test.DatabaseTestUtils;
import ai.lzy.test.GrpcUtils;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.util.grpc.ClientHeaderInterceptor;
import ai.lzy.util.grpc.GrpcHeaders;
import ai.lzy.v1.channel.LCM.ChannelSpec;
import ai.lzy.v1.channel.LCM.DirectChannelType;
import ai.lzy.v1.channel.LCMS.*;
import ai.lzy.v1.channel.LzyChannelManagerGrpc;
import ai.lzy.v1.common.LMD;
import com.google.common.net.HostAndPort;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.micronaut.context.ApplicationContext;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

@SuppressWarnings({"UnstableApiUsage", "ResultOfMethodCallIgnored"})
public class ChannelManagerTest extends BaseTestWithIam {

    @Rule
    public PreparedDbRule iamDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});
    @Rule
    public PreparedDbRule db = EmbeddedPostgresRules.preparedDatabase(ds -> {});

    private ApplicationContext channelManagerCtx;
    @SuppressWarnings("FieldCanBeLocal")
    private ChannelManagerConfig channelManagerConfig;
    private ChannelManager channelManagerApp;
    private ManagedChannel channel;

    private LzyChannelManagerGrpc.LzyChannelManagerBlockingStub unauthorizedChannelManagerClient;
    private LzyChannelManagerGrpc.LzyChannelManagerBlockingStub authorizedChannelManagerClient;

    @Before
    public void before() throws IOException, InterruptedException {
        super.setUp(DatabaseTestUtils.preparePostgresConfig("iam", iamDb.getConnectionInfo()));

        var props = DatabaseTestUtils.preparePostgresConfig("channel-manager", db.getConnectionInfo());
        channelManagerCtx = ApplicationContext.run(props);

        channelManagerConfig = channelManagerCtx.getBean(ChannelManagerConfig.class);
        channelManagerApp = new ChannelManager(channelManagerCtx);
        channelManagerApp.start();

        channel = ChannelBuilder
            .forAddress(HostAndPort.fromString(channelManagerConfig.getAddress()))
            .usePlaintext()
            .build();
        unauthorizedChannelManagerClient = LzyChannelManagerGrpc.newBlockingStub(channel);

        var internalUser = channelManagerConfig.getIam().createCredentials();
        authorizedChannelManagerClient = unauthorizedChannelManagerClient.withInterceptors(
            ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION, internalUser::token));
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

    @Test
    public void testUnauthenticated() {
        try {
            unauthorizedChannelManagerClient.create(ChannelCreateRequest.newBuilder().build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.UNAUTHENTICATED.getCode(), e.getStatus().getCode());
        }

        try {
            unauthorizedChannelManagerClient.destroy(ChannelDestroyRequest.newBuilder().build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.UNAUTHENTICATED.getCode(), e.getStatus().getCode());
        }

        try {
            unauthorizedChannelManagerClient.destroyAll(ChannelDestroyAllRequest.newBuilder().build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.UNAUTHENTICATED.getCode(), e.getStatus().getCode());
        }

        try {
            unauthorizedChannelManagerClient.status(ChannelStatusRequest.newBuilder().build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.UNAUTHENTICATED.getCode(), e.getStatus().getCode());
        }

        try {
            unauthorizedChannelManagerClient.statusAll(ChannelStatusAllRequest.newBuilder().build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.UNAUTHENTICATED.getCode(), e.getStatus().getCode());
        }

        try {
            unauthorizedChannelManagerClient.bind(SlotAttach.newBuilder().build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.UNAUTHENTICATED.getCode(), e.getStatus().getCode());
        }

        try {
            unauthorizedChannelManagerClient.unbind(SlotDetach.newBuilder().build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.UNAUTHENTICATED.getCode(), e.getStatus().getCode());
        }
    }

    @Test
    public void testCreateSuccess() {
        final ChannelCreateResponse channelCreateResponse = authorizedChannelManagerClient.create(
            GrpcUtils.makeCreateDirectChannelCommand(UUID.randomUUID().toString(), "channel1"));
        Assert.assertNotNull(channelCreateResponse.getChannelId());
        Assert.assertTrue(channelCreateResponse.getChannelId().length() > 1);
    }

    @Test
    public void testCreateEmptyWorkflow() {
        try {
            authorizedChannelManagerClient.create(
                ChannelCreateRequest.newBuilder()
                    .setChannelSpec(ChannelSpec.newBuilder().setChannelName("channel1").setDirect(
                            DirectChannelType.newBuilder().build())
                        .setContentType(LMD.DataScheme.newBuilder().setType("text").setSchemeType(
                            LMD.SchemeType.plain.name()).build())
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
            authorizedChannelManagerClient.create(
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
            authorizedChannelManagerClient.create(
                ChannelCreateRequest.newBuilder()
                    .setWorkflowId(UUID.randomUUID().toString())
                    .setChannelSpec(ChannelSpec.newBuilder().setDirect(
                            DirectChannelType.newBuilder().build())
                        .setContentType(LMD.DataScheme.newBuilder()
                            .setType("text")
                            .setSchemeType(LMD.SchemeType.plain.name())
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
            authorizedChannelManagerClient.create(
                ChannelCreateRequest.newBuilder()
                    .setWorkflowId(UUID.randomUUID().toString())
                    .setChannelSpec(ChannelSpec.newBuilder().setChannelName("channel1")
                        .setContentType(LMD.DataScheme.newBuilder()
                            .setType("text")
                            .setSchemeType(LMD.SchemeType.plain.name())
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
            authorizedChannelManagerClient.create(
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
            authorizedChannelManagerClient.create(
                ChannelCreateRequest.newBuilder()
                    .setWorkflowId(UUID.randomUUID().toString())
                    .setChannelSpec(ChannelSpec.newBuilder().setChannelName("channel1").setDirect(
                            DirectChannelType.newBuilder().build())
                        .setContentType(LMD.DataScheme.newBuilder().setSchemeType(
                            LMD.SchemeType.plain.name()).build())
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
            authorizedChannelManagerClient.create(
                ChannelCreateRequest.newBuilder()
                    .setWorkflowId(UUID.randomUUID().toString())
                    .setChannelSpec(ChannelSpec.newBuilder().setChannelName("channel1").setDirect(
                            DirectChannelType.newBuilder().build())
                        .setContentType(LMD.DataScheme.newBuilder().setType("text"))
                        .build())
                    .build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.INVALID_ARGUMENT.getCode(), e.getStatus().getCode());
        }
    }

    @Test
    public void testCreateAndDestroy() {
        final ChannelCreateResponse channelCreateResponse = authorizedChannelManagerClient.create(
            GrpcUtils.makeCreateDirectChannelCommand(UUID.randomUUID().toString(), "channel1"));
        final ChannelDestroyResponse channelDestroyResponse = authorizedChannelManagerClient.destroy(
            GrpcUtils.makeDestroyChannelCommand(channelCreateResponse.getChannelId()));
        try {
            authorizedChannelManagerClient.status(
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
            authorizedChannelManagerClient.status(
                ChannelStatusRequest.newBuilder().setChannelId(UUID.randomUUID().toString()).build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.NOT_FOUND.getCode(), e.getStatus().getCode());
        }
    }

    @Test
    public void testDestroyAll() {
        final String workflowId = UUID.randomUUID().toString();
        final ChannelCreateResponse channel1CreateResponse = authorizedChannelManagerClient.create(
            GrpcUtils.makeCreateDirectChannelCommand(workflowId, "channel1"));
        final ChannelCreateResponse channel2CreateResponse = authorizedChannelManagerClient.create(
            GrpcUtils.makeCreateDirectChannelCommand(UUID.randomUUID().toString(), "channel2"));
        authorizedChannelManagerClient.destroyAll(GrpcUtils.makeDestroyAllCommand(workflowId));

        try {
            authorizedChannelManagerClient.status(
                ChannelStatusRequest.newBuilder().setChannelId(channel1CreateResponse.getChannelId()).build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.NOT_FOUND.getCode(), e.getStatus().getCode());
        }
        final ChannelStatus status = authorizedChannelManagerClient.status(
            ChannelStatusRequest.newBuilder().setChannelId(channel2CreateResponse.getChannelId()).build());
        Assert.assertEquals(channel2CreateResponse.getChannelId(), status.getChannelId());
    }

    @Test
    public void testDestroyAllSameChannelName() {
        final String workflowId = UUID.randomUUID().toString();
        final ChannelCreateResponse channel1CreateResponse = authorizedChannelManagerClient.create(
            GrpcUtils.makeCreateDirectChannelCommand(workflowId, "channel1"));
        final ChannelCreateResponse channel2CreateResponse = authorizedChannelManagerClient.create(
            GrpcUtils.makeCreateDirectChannelCommand(UUID.randomUUID().toString(), "channel1"));
        authorizedChannelManagerClient.destroyAll(GrpcUtils.makeDestroyAllCommand(workflowId));

        try {
            authorizedChannelManagerClient.status(
                ChannelStatusRequest.newBuilder().setChannelId(channel1CreateResponse.getChannelId()).build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.NOT_FOUND.getCode(), e.getStatus().getCode());
        }
        final ChannelStatus status = authorizedChannelManagerClient.status(
            ChannelStatusRequest.newBuilder().setChannelId(channel2CreateResponse.getChannelId()).build());
        Assert.assertEquals(channel2CreateResponse.getChannelId(), status.getChannelId());
    }
}

