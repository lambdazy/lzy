package ai.lzy.channelmanager;

import ai.lzy.channelmanager.db.ChannelManagerDataSource;
import ai.lzy.iam.test.BaseTestWithIam;
import ai.lzy.model.DataScheme;
import ai.lzy.model.db.test.DatabaseTestUtils;
import ai.lzy.v1.channel.v2.LCM;
import ai.lzy.v1.channel.v2.LCMPS;
import ai.lzy.v1.channel.v2.LCMPS.ChannelStatusRequest;
import ai.lzy.v1.channel.v2.LCMS;
import ai.lzy.v1.channel.v2.LzyChannelManagerGrpc;
import ai.lzy.v1.channel.v2.LzyChannelManagerPrivateGrpc;
import ai.lzy.v1.common.LMD;
import ai.lzy.v1.common.LMS;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import com.google.common.net.HostAndPort;
import io.grpc.ManagedChannel;
import io.grpc.Server;
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
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static ai.lzy.longrunning.OperationUtils.awaitOperationDone;
import static ai.lzy.model.UriScheme.LzyFs;
import static ai.lzy.model.db.test.DatabaseTestUtils.preparePostgresConfig;
import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcServer;

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
    private LzyChannelManagerGrpc.LzyChannelManagerBlockingStub publicClient;
    private LongRunningServiceGrpc.LongRunningServiceBlockingStub authorizedOperationApiClient;

    private Server mockedSlotApiServer;
    private HostAndPort mockedSlotApiAddress;

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
            LzyChannelManagerPrivateGrpc.newBlockingStub(channel), "NoAuthPrivateTest", null);

        var internalUserCredentials = config.getIam().createRenewableToken();
        authorizedPrivateClient = newBlockingClient(
            unauthorizedPrivateClient, "AuthPrivateTest", () -> internalUserCredentials.get().token());

        publicClient = newBlockingClient(LzyChannelManagerGrpc.newBlockingStub(channel), "NoAuthTest", () -> internalUserCredentials.get().token()); // TODO not internal

        authorizedOperationApiClient = newBlockingClient(
            LongRunningServiceGrpc.newBlockingStub(channel), "OpTest", () -> internalUserCredentials.get().token());

        mockedSlotApiAddress = HostAndPort.fromString(config.getStubSlotApiAddress());
        var slotService = new SlotServiceMock();
        mockedSlotApiServer = newGrpcServer(mockedSlotApiAddress, null)
            .addService(slotService.slotApiService())
            .addService(slotService.operationService())
            .build();
        mockedSlotApiServer.start();
    }

    @After
    public void after() throws InterruptedException {
        iamTestContext.after();
        app.stop();
        app.awaitTermination();
        mockedSlotApiServer.shutdown();
        mockedSlotApiServer.awaitTermination();
        channel.shutdown();
        channel.awaitTermination(60, TimeUnit.SECONDS);
        DatabaseTestUtils.cleanup(context.getBean(ChannelManagerDataSource.class));
        context.close();
    }

    @Test
    public void testUnauthenticated() {
        try {
            unauthorizedPrivateClient.create(LCMPS.ChannelCreateRequest.newBuilder().build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.UNAUTHENTICATED.getCode(), e.getStatus().getCode());
        }

        try {
            unauthorizedPrivateClient.destroy(LCMPS.ChannelDestroyRequest.newBuilder().build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.UNAUTHENTICATED.getCode(), e.getStatus().getCode());
        }

        try {
            unauthorizedPrivateClient.destroyAll(LCMPS.ChannelDestroyAllRequest.newBuilder().build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.UNAUTHENTICATED.getCode(), e.getStatus().getCode());
        }

        try {
            unauthorizedPrivateClient.status(LCMPS.ChannelStatusRequest.newBuilder().build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.UNAUTHENTICATED.getCode(), e.getStatus().getCode());
        }

        try {
            unauthorizedPrivateClient.statusAll(LCMPS.ChannelStatusAllRequest.newBuilder().build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.UNAUTHENTICATED.getCode(), e.getStatus().getCode());
        }
        
        // TODO public client
    }
    
    @Test
    public void testCreateEmptyWorkflow() {
        try {
            authorizedPrivateClient.create(LCMPS.ChannelCreateRequest.newBuilder()
                    .setChannelSpec(LCM.ChannelSpec.newBuilder()
                        .setChannelName("channel1")
                        .setScheme(ai.lzy.model.grpc.ProtoConverter.toProto(DataScheme.PLAIN))
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
            authorizedPrivateClient.create(LCMPS.ChannelCreateRequest.newBuilder()
                .setExecutionId(UUID.randomUUID().toString())
                    .build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.INVALID_ARGUMENT.getCode(), e.getStatus().getCode());
        }
    }

    @Test
    public void testCreateEmptyChannelName() {
        try {
            authorizedPrivateClient.create(LCMPS.ChannelCreateRequest.newBuilder()
                .setExecutionId(UUID.randomUUID().toString())
                .setChannelSpec(LCM.ChannelSpec.newBuilder()
                    .setScheme(ai.lzy.model.grpc.ProtoConverter.toProto(DataScheme.PLAIN))
                    .build())
                .build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.INVALID_ARGUMENT.getCode(), e.getStatus().getCode());
        }
    }

    @Test
    public void testCreateEmptyScheme() {
        try {
            authorizedPrivateClient.create(LCMPS.ChannelCreateRequest.newBuilder()
                .setExecutionId(UUID.randomUUID().toString())
                .setChannelSpec(LCM.ChannelSpec.newBuilder()
                    .setChannelName("channel1")
                    .build())
                .build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.INVALID_ARGUMENT.getCode(), e.getStatus().getCode());
        }
    }

    @Test
    public void testCreateEmptySchemeSchemeFormat() {
        try {
            authorizedPrivateClient.create(LCMPS.ChannelCreateRequest.newBuilder()
                    .setExecutionId(UUID.randomUUID().toString())
                    .setChannelSpec(LCM.ChannelSpec.newBuilder()
                        .setChannelName("channel1")
                        .setScheme(LMD.DataScheme.newBuilder().setDataFormat("raw_type").build())
                        .build())
                    .build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.INVALID_ARGUMENT.getCode(), e.getStatus().getCode());
        }
    }

    @Test
    public void testCreateEmptySchemeDataFormat() {
        try {
            authorizedPrivateClient.create(LCMPS.ChannelCreateRequest.newBuilder()
                    .setExecutionId(UUID.randomUUID().toString())
                    .setChannelSpec(LCM.ChannelSpec.newBuilder()
                        .setChannelName("channel1")
                        .setScheme(LMD.DataScheme.newBuilder().setSchemeFormat("no_schema").build())
                        .build())
                    .build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.INVALID_ARGUMENT.getCode(), e.getStatus().getCode());
        }
    }

    @Test
    public void testCreateDestroy() {
        final String executionId = UUID.randomUUID().toString();
        final String channelName = "ch1";
        final LCMPS.ChannelCreateResponse channelCreateResponse = authorizedPrivateClient.create(
            makeChannelCreateCommand(executionId, channelName));
        final String channelId = channelCreateResponse.getChannelId();
        Assert.assertTrue(channelId.length() > 1);

        final LCMPS.ChannelStatusResponse channelStatusResponse = authorizedPrivateClient.status(
            ChannelStatusRequest.newBuilder().setChannelId(channelCreateResponse.getChannelId()).build());
        final var expectedChannelStatus = LCMPS.ChannelStatus.newBuilder()
            .setChannel(LCM.Channel.newBuilder()
                .setChannelId(channelId)
                .setSpec(makeChannelSpec(channelName))
                .setExecutionId(executionId)
                .setSenders(LCM.ChannelSenders.getDefaultInstance())
                .setReceivers(LCM.ChannelReceivers.getDefaultInstance())
                .build())
            .build();
        Assert.assertEquals(expectedChannelStatus, channelStatusResponse.getStatus());

        LongRunning.Operation destroyOp = authorizedPrivateClient.destroy(
            makeChannelDestroyCommand(channelCreateResponse.getChannelId()));
        destroyOp = awaitOperationDone(authorizedOperationApiClient, destroyOp.getId(),
            Duration.of(10, ChronoUnit.SECONDS));
        Assert.assertTrue(destroyOp.hasResponse());
        try {
            authorizedPrivateClient.status(makeChannelStatusCommand(channelCreateResponse.getChannelId()));
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.NOT_FOUND.getCode(), e.getStatus().getCode());
        }
    }

    @Test
    public void testDestroyNonexistentChannel() {
        final String channelId = UUID.randomUUID().toString();
        LongRunning.Operation destroyOp = authorizedPrivateClient.destroy(makeChannelDestroyCommand(channelId));
        destroyOp = awaitOperationDone(authorizedOperationApiClient, destroyOp.getId(),
            Duration.of(10, ChronoUnit.SECONDS));
        Assert.assertTrue(destroyOp.hasResponse());
    }

    @Test
    public void testDestroyAll() {
        final String executionId = UUID.randomUUID().toString();
        final LCMPS.ChannelCreateResponse ch1Response = authorizedPrivateClient.create(
            makeChannelCreateCommand(executionId, "ch1"));
        final LCMPS.ChannelCreateResponse ch2Response = authorizedPrivateClient.create(
            makeChannelCreateCommand(executionId, "ch2"));
        final LCMPS.ChannelCreateResponse ch0Response = authorizedPrivateClient.create(
            makeChannelCreateCommand(UUID.randomUUID().toString(), "ch0"));

        LongRunning.Operation destroyAllOp = authorizedPrivateClient.destroyAll(makeChannelDestroyAllCommand(executionId));
        destroyAllOp = awaitOperationDone(authorizedOperationApiClient, destroyAllOp.getId(),
            Duration.of(10, ChronoUnit.SECONDS));
        Assert.assertTrue(destroyAllOp.hasResponse());
        try {
            authorizedPrivateClient.status(makeChannelStatusCommand(ch1Response.getChannelId()));
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.NOT_FOUND.getCode(), e.getStatus().getCode());
        }
        try {
            authorizedPrivateClient.status(makeChannelStatusCommand(ch2Response.getChannelId()));
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.NOT_FOUND.getCode(), e.getStatus().getCode());
        }
        final var status = authorizedPrivateClient.status(makeChannelStatusCommand(ch0Response.getChannelId()));
        Assert.assertEquals(ch0Response.getChannelId(), status.getStatus().getChannel().getChannelId());
    }

    @Test
    public void testBindUnbindSingle() {
        final String executionId = UUID.randomUUID().toString();
        final String channelName = "ch";
        final LCMPS.ChannelCreateResponse chResponse = authorizedPrivateClient.create(
            makeChannelCreateCommand(executionId, channelName));

        final var bindRequest = makeBindCommand(chResponse.getChannelId(), "slot",
            LMS.Slot.Direction.INPUT, LCMS.BindRequest.SlotOwner.WORKER);
        LongRunning.Operation bindOp = publicClient.bind(bindRequest);
        bindOp = awaitOperationDone(authorizedOperationApiClient, bindOp.getId(),
            Duration.of(10, ChronoUnit.SECONDS));
        Assert.assertTrue(bindOp.hasResponse());

        var status = authorizedPrivateClient.status(makeChannelStatusCommand(chResponse.getChannelId()));
        Assert.assertEquals(1, status.getStatus().getChannel().getReceivers().getWorkerSlotsCount());

        final var unbindRequest = makeUnbindCommand(bindRequest.getSlotInstance().getSlotUri());
        LongRunning.Operation unbindOp = publicClient.unbind(unbindRequest);
        unbindOp = awaitOperationDone(authorizedOperationApiClient, unbindOp.getId(),
            Duration.of(10, ChronoUnit.SECONDS));
        Assert.assertTrue(unbindOp.hasResponse());

        status = authorizedPrivateClient.status(makeChannelStatusCommand(chResponse.getChannelId()));
        Assert.assertEquals(0, status.getStatus().getChannel().getReceivers().getWorkerSlotsCount());
    }

    private LCMPS.ChannelCreateRequest makeChannelCreateCommand(String executionId, String channelName) {
        return LCMPS.ChannelCreateRequest.newBuilder()
            .setExecutionId(executionId)
            .setChannelSpec(makeChannelSpec(channelName))
            .build();
    }

    private LCM.ChannelSpec makeChannelSpec(String channelName) {
        return LCM.ChannelSpec.newBuilder()
            .setChannelName(channelName)
            .setScheme(ai.lzy.model.grpc.ProtoConverter.toProto(DataScheme.PLAIN))
            .build();
    }

    private LCMPS.ChannelDestroyRequest makeChannelDestroyCommand(String channelId) {
        return LCMPS.ChannelDestroyRequest.newBuilder()
            .setChannelId(channelId)
            .build();
    }

    private LCMPS.ChannelDestroyAllRequest makeChannelDestroyAllCommand(String executionId) {
        return LCMPS.ChannelDestroyAllRequest.newBuilder()
            .setExecutionId(executionId)
            .build();
    }

    private LCMPS.ChannelStatusRequest makeChannelStatusCommand(String channelId) {
        return LCMPS.ChannelStatusRequest.newBuilder()
            .setChannelId(channelId)
            .build();
    }

    private LCMS.BindRequest makeBindCommand(String channelId, String slotName, LMS.Slot.Direction slotDirection, LCMS.BindRequest.SlotOwner slotOwner) {
        URI slotUri = URI.create("%s://%s:%d".formatted(LzyFs.scheme(),
            mockedSlotApiAddress.getHost(), mockedSlotApiAddress.getPort()
        )).resolve(Path.of("/", "tid", slotName).toString());
        return LCMS.BindRequest.newBuilder()
            .setSlotInstance(LMS.SlotInstance.newBuilder()
                .setTaskId("tid")
                .setSlot(LMS.Slot.newBuilder()
                    .setName(slotName)
                    .setDirection(slotDirection)
                    .setMedia(LMS.Slot.Media.PIPE)
                    .setContentType(ai.lzy.model.grpc.ProtoConverter.toProto(DataScheme.PLAIN))
                    .build())
                .setSlotUri(slotUri.toString())
                .setChannelId(channelId)
                .build())
            .setSlotOwner(slotOwner)
            .build();
    }

    private LCMS.UnbindRequest makeUnbindCommand(String slotUri) {
        return LCMS.UnbindRequest.newBuilder()
            .setSlotUri(slotUri)
            .build();
    }

}

