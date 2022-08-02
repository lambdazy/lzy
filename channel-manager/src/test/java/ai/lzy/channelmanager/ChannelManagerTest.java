package ai.lzy.channelmanager;

import ai.lzy.model.grpc.ChannelBuilder;
import ai.lzy.model.grpc.ClientHeaderInterceptor;
import ai.lzy.model.grpc.GrpcHeaders;
import ai.lzy.test.BaseTestWithIam;
import ai.lzy.test.JwtUtils;
import ai.lzy.v1.ChannelManager.ChannelCreateRequest;
import ai.lzy.v1.ChannelManager.ChannelDestroyAllRequest;
import ai.lzy.v1.ChannelManager.ChannelDestroyRequest;
import ai.lzy.v1.ChannelManager.ChannelStatusRequest;
import ai.lzy.v1.ChannelManager.ChannelsStatusRequest;
import ai.lzy.v1.ChannelManager.SlotAttach;
import ai.lzy.v1.ChannelManager.SlotDetach;
import ai.lzy.v1.Channels.ChannelSpec;
import ai.lzy.v1.Channels.DirectChannelType;
import ai.lzy.v1.LzyChannelManagerGrpc;
import ai.lzy.v1.Operations.DataScheme;
import ai.lzy.v1.Operations.SchemeType;
import com.google.common.net.HostAndPort;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.micronaut.context.ApplicationContext;
import java.io.IOException;
import java.util.UUID;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings({"UnstableApiUsage", "ResultOfMethodCallIgnored"})
public class ChannelManagerTest extends BaseTestWithIam {

    private ApplicationContext channelManagerCtx;
    @SuppressWarnings("FieldCanBeLocal")
    private ChannelManagerConfig channelManagerConfig;
    private ChannelManager channelManagerApp;

    private LzyChannelManagerGrpc.LzyChannelManagerBlockingStub unauthorizedChannelManagerClient;
    private LzyChannelManagerGrpc.LzyChannelManagerBlockingStub authorizedChannelManagerClient;

    @Before
    public void before() throws IOException {
        super.before();
        channelManagerCtx = ApplicationContext.run();
        channelManagerConfig = channelManagerCtx.getBean(ChannelManagerConfig.class);
        channelManagerApp = new ChannelManager(channelManagerCtx);
        channelManagerApp.start();

        var channel = ChannelBuilder
            .forAddress(HostAndPort.fromString(channelManagerConfig.address()))
            .usePlaintext()
            .build();
        unauthorizedChannelManagerClient = LzyChannelManagerGrpc.newBlockingStub(channel);
        authorizedChannelManagerClient = unauthorizedChannelManagerClient.withInterceptors(
            ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION, JwtUtils.invalidCredentials("user")::token));
    }

    @After
    public void after() {
        channelManagerApp.close();
        try {
            channelManagerApp.awaitTermination();
        } catch (InterruptedException ignored) {
            // ignored
        }
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
            unauthorizedChannelManagerClient.channelsStatus(ChannelsStatusRequest.newBuilder().build());
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
    public void testCreate() {
        authorizedChannelManagerClient.create(ChannelCreateRequest.newBuilder()
            .setWorkflowId(UUID.randomUUID().toString())
            .setChannelSpec(ChannelSpec.newBuilder().setChannelName("channel1").setDirect(
                    DirectChannelType.newBuilder().build())
                .setContentType(DataScheme.newBuilder().setType("text").setSchemeType(
                    SchemeType.plain).build())
                .build())
            .build());
    }
}
