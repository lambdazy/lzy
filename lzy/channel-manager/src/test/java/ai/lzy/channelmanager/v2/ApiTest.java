package ai.lzy.channelmanager.v2;

import ai.lzy.channelmanager.ChannelManagerBaseApiTest;
import ai.lzy.channelmanager.config.ChannelManagerConfig;
import ai.lzy.channelmanager.test.InjectedFailures;
import ai.lzy.iam.test.BaseTestWithIam;
import ai.lzy.v1.channel.v2.LCMPS;
import ai.lzy.v1.channel.v2.LCMS;
import ai.lzy.v1.channel.v2.LzyChannelManagerGrpc;
import ai.lzy.v1.channel.v2.LzyChannelManagerPrivateGrpc;
import ai.lzy.v1.common.LC.PeerDescription.StoragePeer;
import com.google.common.net.HostAndPort;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.micronaut.context.ApplicationContext;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import org.junit.*;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static ai.lzy.model.db.test.DatabaseTestUtils.preparePostgresConfig;
import static ai.lzy.util.grpc.GrpcUtils.*;
import static ai.lzy.util.grpc.GrpcUtils.NO_AUTH;

public class ApiTest {
    private static final BaseTestWithIam iamTestContext = new BaseTestWithIam();

    @ClassRule
    public static PreparedDbRule iamDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});
    @ClassRule
    public static PreparedDbRule channelManagerDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});

    private static ApplicationContext context;
    private static ChannelManagerMain app;
    private static ManagedChannel channel;
    private static String workflowName;
    private static ChannelManagerBaseApiTest.User user;
    private static LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateBlockingStub privateClient;
    private static LzyChannelManagerGrpc.LzyChannelManagerBlockingStub publicClient;
    private static HostAndPort mockedSlotApiAddress;
    private static Server mockedSlotApiServer;
    private static SlotsApiMock slotService;
    private static Server mockedLzyServiceServer;

    @BeforeClass
    public static void before() throws Exception {
        var iamDbConfig = preparePostgresConfig("iam", iamDb.getConnectionInfo());
        iamTestContext.setUp(iamDbConfig);

        var channelManagerDbConfig = preparePostgresConfig("channel-manager", channelManagerDb.getConnectionInfo());
        context = ApplicationContext.run(channelManagerDbConfig);
        var config = context.getBean(ChannelManagerConfig.class);
        config.getIam().setAddress("localhost:" + iamTestContext.getPort());

        app = context.getBean(ChannelManagerMain.class);
        app.start();

        channel = newGrpcChannel(config.getAddress(), LzyChannelManagerPrivateGrpc.SERVICE_NAME);

        workflowName = "wfName";
        try (final var iamClient = new ChannelManagerBaseApiTest.IamClient(config.getIam())) {
            user = iamClient.createUser("workflowUser");
            iamClient.addWorkflowAccess(user, workflowName);
        }

        var internalUserCredentials = config.getIam().createRenewableToken();
        privateClient = newBlockingClient(LzyChannelManagerPrivateGrpc.newBlockingStub(channel),
            "AuthPrivateTest", () -> internalUserCredentials.get().token());

        publicClient = newBlockingClient(LzyChannelManagerGrpc.newBlockingStub(channel),
            "AuthPublicTest", () -> user.credentials().token());

        mockedSlotApiAddress = HostAndPort.fromString(config.getStubSlotApiAddress());
        slotService = new SlotsApiMock();
        mockedSlotApiServer = newGrpcServer(mockedSlotApiAddress, NO_AUTH)
            .addService(slotService)
            .build();
        mockedSlotApiServer.start();

        var mockedLzyServiceAddress = HostAndPort.fromString(config.getLzyServiceAddress());
        mockedLzyServiceServer = newGrpcServer(mockedLzyServiceAddress, NO_AUTH)
            .addService(new LzyServiceMock())
            .build();
        mockedLzyServiceServer.start();
    }

    @AfterClass
    public static void afterClass() throws InterruptedException {
        InjectedFailures.assertClean();

        iamTestContext.after();
        app.stop();
        app.awaitTermination();
        channel.shutdown();
        channel.awaitTermination(10, TimeUnit.SECONDS);
        context.close();

        mockedSlotApiServer.shutdown();
        mockedSlotApiServer.awaitTermination();

        mockedLzyServiceServer.shutdownNow();
        mockedLzyServiceServer.awaitTermination();
    }

    @After
    public void after() {
        privateClient.destroyAll(LCMPS.DestroyAllRequest.newBuilder()
            .setExecutionId("execId")
            .build());
    }

    @Test
    public void testSimple() throws Exception {
        var chan = privateClient.getOrCreate(
            LCMPS.GetOrCreateRequest.newBuilder()
                .setExecutionId("execId")
                .setWorkflowName(workflowName)
                .setUserId(user.id())
                .setProducer(StoragePeer.newBuilder()
                    .setStorageUri("s3://some-bucket")
                    .build())
                .build());

        var resp = bind(chan.getChannelId(), LCMS.BindRequest.Role.CONSUMER, "1");

        Assert.assertEquals("s3://some-bucket", resp.getPeer().getStoragePeer().getStorageUri());

        var state = publicClient.getChannelsStatus(
            LCMS.GetChannelsStatusRequest.newBuilder()
                .addAllChannelIds(List.of(chan.getChannelId()))
                .setExecutionId("execId")
                .build());

        Assert.assertEquals(1, state.getChannelsCount());
        Assert.assertEquals(1, state.getChannels(0).getConsumersCount());
        Assert.assertEquals(1, state.getChannels(0).getProducersCount());

        Assert.assertEquals(resp.getPeer().getPeerId(), state.getChannels(0).getProducers(0).getPeerId());
        Assert.assertEquals("1", state.getChannels(0).getConsumers(0).getPeerId());

        completed(chan.getChannelId(), resp.getTransferId());
    }

    private static void completed(String chanId, String transferId) {
        publicClient.transferCompleted(
            LCMS.TransferCompletedRequest.newBuilder()
                .setChannelId(chanId)
                .setTransferId(transferId)
                .build());
    }

    @Test
    public void testStorageConsumer() {
        var chan = privateClient.getOrCreate(
            LCMPS.GetOrCreateRequest.newBuilder()
                .setExecutionId("execId")
                .setWorkflowName(workflowName)
                .setUserId(user.id())
                .setConsumer(StoragePeer.newBuilder()
                    .setStorageUri("s3://some-bucket")
                    .build())
                .build());

        var resp = bind(chan.getChannelId(), LCMS.BindRequest.Role.PRODUCER, "1");

        Assert.assertEquals("s3://some-bucket", resp.getPeer().getStoragePeer().getStorageUri());

        completed(chan.getChannelId(), resp.getTransferId());

        var state = publicClient.getChannelsStatus(
            LCMS.GetChannelsStatusRequest.newBuilder()
                .addAllChannelIds(List.of(chan.getChannelId()))
                .setExecutionId("execId")
                .build());

        Assert.assertEquals(1, state.getChannelsCount());
        Assert.assertEquals(0, state.getChannels(0).getConsumersCount());
        Assert.assertEquals(2, state.getChannels(0).getProducersCount());  // storage reconnected as producer

        // Connecting new consumer to get slot producer
        var resp2 = bind(chan.getChannelId(), LCMS.BindRequest.Role.CONSUMER, "2");

        Assert.assertEquals("1", resp2.getPeer().getPeerId());  // New producer is most prioritized
    }

    @Test
    public void testEarlyConsumer() throws ExecutionException, InterruptedException {
        var chan = privateClient.getOrCreate(
            LCMPS.GetOrCreateRequest.newBuilder()
                .setExecutionId("execId")
                .setWorkflowName(workflowName)
                .setUserId(user.id())
                .setConsumer(StoragePeer.newBuilder()
                    .setStorageUri("s3://some-bucket")
                    .build())
                .build());

        var fut = slotService.waitForStartTransfer("1");
        var resp = bind(chan.getChannelId(), LCMS.BindRequest.Role.CONSUMER, "1");
        Assert.assertFalse(resp.hasPeer());

        var resp2 = bind(chan.getChannelId(), LCMS.BindRequest.Role.PRODUCER, "2");
        Assert.assertEquals("s3://some-bucket", resp2.getPeer().getStoragePeer().getStorageUri());

        var resp3 = fut.get();
        Assert.assertEquals("2", resp3.getPeer().getPeerId());
    }

    @Test
    public void testPriorityAfterFail() {
        var chan = privateClient.getOrCreate(
            LCMPS.GetOrCreateRequest.newBuilder()
                .setExecutionId("execId")
                .setWorkflowName(workflowName)
                .setProducer(StoragePeer.newBuilder()
                    .setStorageUri("s3://some-bucket")
                    .build())
                .setUserId(user.id())
                .build());

        // Creating producers
        bind(chan.getChannelId(), LCMS.BindRequest.Role.PRODUCER, "1");
        bind(chan.getChannelId(), LCMS.BindRequest.Role.PRODUCER, "2");

        // Binding consumer
        var resp3 = bind(chan.getChannelId(), LCMS.BindRequest.Role.CONSUMER, "3");
        var prodId = resp3.getPeer().getPeerId();

        // Failing transfer
        var resp4 = publicClient.transferFailed(
            LCMS.TransferFailedRequest.newBuilder()
                .setChannelId(chan.getChannelId())
                .setDescription("Fail")
                .setTransferId(resp3.getTransferId())
                .build());

        Assert.assertTrue(resp4.hasNewPeer());
        Assert.assertNotEquals(prodId, resp4.getNewPeer().getPeerId());
    }

    @Test
    public void testGetExistingChannel() {
        var channel = privateClient.getOrCreate(
            LCMPS.GetOrCreateRequest.newBuilder()
                .setExecutionId("execId")
                .setWorkflowName(workflowName)
                .setProducer(StoragePeer.newBuilder()
                    .setStorageUri("s3://some-bucket")
                    .build())
                .setUserId(user.id())
                .build());

        var channel2 = privateClient.getOrCreate(
            LCMPS.GetOrCreateRequest.newBuilder()
                .setExecutionId("execId")
                .setWorkflowName(workflowName)
                .setProducer(StoragePeer.newBuilder()
                    .setStorageUri("s3://some-bucket")
                    .build())
                .setUserId(user.id())
                .build());

        Assert.assertEquals(channel.getChannelId(), channel2.getChannelId());
    }

    @Test
    public void testFailedToLoadDataToStorage() {
        var chan = privateClient.getOrCreate(
            LCMPS.GetOrCreateRequest.newBuilder()
                .setExecutionId("execId")
                .setWorkflowName(workflowName)
                .setConsumer(StoragePeer.newBuilder()
                    .setStorageUri("s3://some-bucket")
                    .build())
                .setUserId(user.id())
                .build());

        var resp = bind(chan.getChannelId(), LCMS.BindRequest.Role.PRODUCER, "1");

        try {
            publicClient.transferFailed(
                LCMS.TransferFailedRequest.newBuilder()
                    .setChannelId(chan.getChannelId())
                    .setDescription("Fail")
                    .setTransferId(resp.getTransferId())
                    .build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(Status.Code.INTERNAL, e.getStatus().getCode());
        }
    }

    @Test
    public void testUnbind() {
        var chan = privateClient.getOrCreate(
            LCMPS.GetOrCreateRequest.newBuilder()
                .setExecutionId("execId")
                .setWorkflowName(workflowName)
                .setProducer(StoragePeer.newBuilder()
                    .setStorageUri("s3://some-bucket")
                    .build())
                .setUserId(user.id())
                .build());

        bind(chan.getChannelId(), LCMS.BindRequest.Role.PRODUCER, "1");

        publicClient.unbind(
            LCMS.UnbindRequest.newBuilder()
                .setChannelId(chan.getChannelId())
                .setPeerId("1")
                .build());

        var status = publicClient.getChannelsStatus(
            LCMS.GetChannelsStatusRequest.newBuilder()
                .addAllChannelIds(List.of(chan.getChannelId()))
                .setExecutionId("execId")
                .build());

        Assert.assertEquals(1, status.getChannelsCount());
        Assert.assertEquals(1, status.getChannels(0).getProducersCount());
        Assert.assertEquals(0, status.getChannels(0).getConsumersCount());
    }


    private static LCMS.BindResponse bind(String chanId, LCMS.BindRequest.Role producer, String value) {
        return publicClient.bind(
            LCMS.BindRequest.newBuilder()
                .setChannelId(chanId)
                .setExecutionId("execId")
                .setRole(producer)
                .setPeerId(value)
                .setPeerUrl("http://" + mockedSlotApiAddress.toString())
                .build());
    }

}
