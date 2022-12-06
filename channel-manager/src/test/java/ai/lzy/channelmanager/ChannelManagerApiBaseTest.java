package ai.lzy.channelmanager;

import ai.lzy.channelmanager.db.ChannelManagerDataSource;
import ai.lzy.channelmanager.v2.config.ChannelManagerConfig;
import ai.lzy.iam.test.BaseTestWithIam;
import ai.lzy.longrunning.OperationUtils;
import ai.lzy.model.DataScheme;
import ai.lzy.model.db.test.DatabaseTestUtils;
import ai.lzy.v1.channel.v2.LCM.Channel;
import ai.lzy.v1.channel.v2.LCM.ChannelSpec;
import ai.lzy.v1.channel.v2.LCMPS.ChannelCreateRequest;
import ai.lzy.v1.channel.v2.LCMPS.ChannelDestroyAllRequest;
import ai.lzy.v1.channel.v2.LCMPS.ChannelDestroyRequest;
import ai.lzy.v1.channel.v2.LCMPS.ChannelStatusRequest;
import ai.lzy.v1.channel.v2.LCMS.BindRequest;
import ai.lzy.v1.channel.v2.LCMS.UnbindRequest;
import ai.lzy.v1.channel.v2.LzyChannelManagerGrpc;
import ai.lzy.v1.channel.v2.LzyChannelManagerPrivateGrpc;
import ai.lzy.v1.common.LMS;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import com.google.common.net.HostAndPort;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.micronaut.context.ApplicationContext;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

import static ai.lzy.model.UriScheme.LzyFs;
import static ai.lzy.model.db.test.DatabaseTestUtils.preparePostgresConfig;
import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcServer;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ChannelManagerApiBaseTest {
    private static final BaseTestWithIam iamTestContext = new BaseTestWithIam();

    @Rule
    public PreparedDbRule iamDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});
    @Rule
    public PreparedDbRule channelManagerDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});

    private ApplicationContext context;
    private ChannelManagerConfig config;
    private ChannelManagerApp app;

    protected ManagedChannel channel;
    protected LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateBlockingStub privateClient;
    protected LzyChannelManagerGrpc.LzyChannelManagerBlockingStub publicClient;
    protected LongRunningServiceGrpc.LongRunningServiceBlockingStub operationApiClient;

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


        var internalUserCredentials = config.getIam().createRenewableToken();
        privateClient = newBlockingClient(LzyChannelManagerPrivateGrpc.newBlockingStub(channel),
            "AuthPrivateTest", () -> internalUserCredentials.get().token());

        publicClient = newBlockingClient(LzyChannelManagerGrpc.newBlockingStub(channel),
            "NoAuthTest", () -> internalUserCredentials.get().token()); // TODO not internal

        operationApiClient = newBlockingClient(
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

    protected ChannelCreateRequest makeChannelCreateCommand(String executionId, String channelName) {
        return ChannelCreateRequest.newBuilder()
            .setExecutionId(executionId)
            .setChannelSpec(makeChannelSpec(channelName))
            .build();
    }

    protected ChannelSpec makeChannelSpec(String channelName) {
        return ChannelSpec.newBuilder()
            .setChannelName(channelName)
            .setScheme(ai.lzy.model.grpc.ProtoConverter.toProto(DataScheme.PLAIN))
            .build();
    }

    protected ChannelDestroyRequest makeChannelDestroyCommand(String channelId) {
        return ChannelDestroyRequest.newBuilder()
            .setChannelId(channelId)
            .build();
    }

    protected ChannelDestroyAllRequest makeChannelDestroyAllCommand(String executionId) {
        return ChannelDestroyAllRequest.newBuilder()
            .setExecutionId(executionId)
            .build();
    }

    protected ChannelStatusRequest makeChannelStatusCommand(String channelId) {
        return ChannelStatusRequest.newBuilder()
            .setChannelId(channelId)
            .build();
    }

    protected BindRequest makeBindCommand(String channelId, String slotName,
                                          LMS.Slot.Direction slotDirection, BindRequest.SlotOwner slotOwner)
    {
        URI slotUri = URI.create("%s://%s:%d".formatted(LzyFs.scheme(),
            mockedSlotApiAddress.getHost(), mockedSlotApiAddress.getPort()
        )).resolve(Path.of("/", "tid", slotName).toString());

        return BindRequest.newBuilder()
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

    protected UnbindRequest makeUnbindCommand(String slotUri) {
        return UnbindRequest.newBuilder()
            .setSlotUri(slotUri)
            .build();
    }

    protected LongRunning.Operation awaitOperationResponse(String operationId) {
        LongRunning.Operation operation = OperationUtils.awaitOperationDone(
            operationApiClient, operationId, Duration.of(10, ChronoUnit.SECONDS));
        assertTrue(operation.hasResponse());
        return operation;
    }

    protected void assertChannelShape(int expectedPortalSenders, int expectedWorkerSenders,
                                      int expectedPortalReceivers, int expectedWorkerReceivers,
                                      Channel actualChannel)
    {
        assertEquals("Expected " + expectedPortalSenders + " PORTAL senders",
            expectedPortalSenders, actualChannel.getSenders().hasPortalSlot() ? 1 : 0);
        assertEquals("Expected " + expectedWorkerSenders + " WORKER senders",
            expectedWorkerSenders, actualChannel.getSenders().hasWorkerSlot() ? 1 : 0);
        assertEquals("Expected " + expectedPortalReceivers + " PORTAL receivers",
            expectedPortalReceivers, actualChannel.getReceivers().hasPortalSlot() ? 1 : 0);
        assertEquals("Expected " + expectedWorkerReceivers + " WORKER receivers",
            expectedWorkerReceivers, actualChannel.getReceivers().getWorkerSlotsCount());
    }

}

