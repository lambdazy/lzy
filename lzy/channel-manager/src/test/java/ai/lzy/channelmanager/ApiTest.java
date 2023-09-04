package ai.lzy.channelmanager;

import ai.lzy.channelmanager.config.ChannelManagerConfig;
import ai.lzy.channelmanager.test.InjectedFailures;
import ai.lzy.iam.clients.AccessBindingClient;
import ai.lzy.iam.clients.SubjectServiceClient;
import ai.lzy.iam.config.IamClientConfiguration;
import ai.lzy.iam.grpc.client.AccessBindingServiceGrpcClient;
import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.iam.resources.AccessBinding;
import ai.lzy.iam.resources.Role;
import ai.lzy.iam.resources.credentials.SubjectCredentials;
import ai.lzy.iam.resources.impl.Workflow;
import ai.lzy.iam.resources.subjects.AuthProvider;
import ai.lzy.iam.resources.subjects.CredentialsType;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.util.auth.credentials.CredentialsUtils;
import ai.lzy.util.auth.credentials.JwtCredentials;
import ai.lzy.util.auth.credentials.JwtUtils;
import ai.lzy.util.auth.credentials.RsaUtils;
import ai.lzy.util.grpc.GrpcUtils;
import ai.lzy.v1.channel.LCMPS;
import ai.lzy.v1.channel.LCMS;
import ai.lzy.v1.channel.LzyChannelManagerGrpc;
import ai.lzy.v1.common.LC;
import ai.lzy.v1.iam.LzyAuthenticateServiceGrpc;
import com.google.common.net.HostAndPort;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static ai.lzy.util.grpc.GrpcUtils.*;

public class ApiTest extends IamOnlyChannelManagerContextTests {
    private static String workflowName;
    private static User user;

    private static HostAndPort mockedSlotApiAddress;
    private static Server mockedSlotApiServer;
    private static SlotsApiMock slotService;
    private static Server mockedLzyServiceServer;

    private static LzyChannelManagerGrpc.LzyChannelManagerBlockingStub publicClient;

    @BeforeClass
    public static void before() throws Exception {
        var config = context.getBean(ChannelManagerConfig.class);

        workflowName = "wfName";
        try (final var iamClient = new ApiTest.IamClient(config.getIam())) {
            user = iamClient.createUser("workflowUser");
            iamClient.addWorkflowAccess(user, workflowName);
        }

        publicClient = newBlockingClient(LzyChannelManagerGrpc.newBlockingStub(channelManagerGrpcChannel),
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
                .setProducer(LC.PeerDescription.StoragePeer.newBuilder()
                    .setStorageUri("s3://some-bucket")
                    .build())
                .build());

        var resp = bind(chan.getChannelId(), LCMS.BindRequest.Role.CONSUMER, "1", "bind");

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

        completeTransfer(chan.getChannelId(), resp.getTransferId(), "complete");
    }

    private static void completeTransfer(String chanId, String transferId, String idempotencyKey) {
        withIdempotencyKey(publicClient, idempotencyKey).transferCompleted(
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
                .setConsumer(LC.PeerDescription.StoragePeer.newBuilder()
                    .setStorageUri("s3://some-bucket")
                    .build())
                .build());

        var resp = bind(chan.getChannelId(), LCMS.BindRequest.Role.PRODUCER, "1", "bind-1");

        Assert.assertEquals("s3://some-bucket", resp.getPeer().getStoragePeer().getStorageUri());

        completeTransfer(chan.getChannelId(), resp.getTransferId(), "complete");

        var state = publicClient.getChannelsStatus(
            LCMS.GetChannelsStatusRequest.newBuilder()
                .addAllChannelIds(List.of(chan.getChannelId()))
                .setExecutionId("execId")
                .build());

        Assert.assertEquals(1, state.getChannelsCount());
        Assert.assertEquals(0, state.getChannels(0).getConsumersCount());
        Assert.assertEquals(2, state.getChannels(0).getProducersCount());  // storage reconnected as producer

        // Connecting new consumer to get slot producer
        var resp2 = bind(chan.getChannelId(), LCMS.BindRequest.Role.CONSUMER, "2", "bind-2");

        Assert.assertEquals("1", resp2.getPeer().getPeerId());  // New producer is most prioritized
    }

    @Test
    public void testEarlyConsumer() throws ExecutionException, InterruptedException {
        var chan = privateClient.getOrCreate(
            LCMPS.GetOrCreateRequest.newBuilder()
                .setExecutionId("execId")
                .setWorkflowName(workflowName)
                .setUserId(user.id())
                .setConsumer(LC.PeerDescription.StoragePeer.newBuilder()
                    .setStorageUri("s3://some-bucket")
                    .build())
                .build());

        var fut = slotService.waitForStartTransfer("1");
        var resp = bind(chan.getChannelId(), LCMS.BindRequest.Role.CONSUMER, "1", "bind-1");
        Assert.assertFalse(resp.hasPeer());

        var resp2 = bind(chan.getChannelId(), LCMS.BindRequest.Role.PRODUCER, "2", "bind-2");
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
                .setProducer(LC.PeerDescription.StoragePeer.newBuilder()
                    .setStorageUri("s3://some-bucket")
                    .build())
                .setUserId(user.id())
                .build());

        // Creating producers
        bind(chan.getChannelId(), LCMS.BindRequest.Role.PRODUCER, "1", "bind-1");
        bind(chan.getChannelId(), LCMS.BindRequest.Role.PRODUCER, "2", "bind-2");

        // Binding consumer
        var resp3 = bind(chan.getChannelId(), LCMS.BindRequest.Role.CONSUMER, "3", "bind-3");
        var prodId = resp3.getPeer().getPeerId();

        // Failing transfer
        var resp4 = withIdempotencyKey(publicClient, "failed").transferFailed(
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
                .setProducer(LC.PeerDescription.StoragePeer.newBuilder()
                    .setStorageUri("s3://some-bucket")
                    .build())
                .setUserId(user.id())
                .build());

        var channel2 = privateClient.getOrCreate(
            LCMPS.GetOrCreateRequest.newBuilder()
                .setExecutionId("execId")
                .setWorkflowName(workflowName)
                .setProducer(LC.PeerDescription.StoragePeer.newBuilder()
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
                .setConsumer(LC.PeerDescription.StoragePeer.newBuilder()
                    .setStorageUri("s3://some-bucket")
                    .build())
                .setUserId(user.id())
                .build());

        var resp = bind(chan.getChannelId(), LCMS.BindRequest.Role.PRODUCER, "1", "bind-1");

        try {
            withIdempotencyKey(publicClient, "failed").transferFailed(
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
                .setProducer(LC.PeerDescription.StoragePeer.newBuilder()
                    .setStorageUri("s3://some-bucket")
                    .build())
                .setUserId(user.id())
                .build());

        bind(chan.getChannelId(), LCMS.BindRequest.Role.PRODUCER, "1", "bind");

        withIdempotencyKey(publicClient, "unbind").unbind(
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


    private static LCMS.BindResponse bind(String chanId, LCMS.BindRequest.Role producer, String value,
                                          String idempotencyKey)
    {
        return withIdempotencyKey(publicClient, idempotencyKey).bind(
            LCMS.BindRequest.newBuilder()
                .setChannelId(chanId)
                .setExecutionId("execId")
                .setRole(producer)
                .setPeerId(value)
                .setPeerUrl("http://" + mockedSlotApiAddress.toString())
                .build());
    }

    public record User(
        String id,
        JwtCredentials credentials
    ) {}

    public static class IamClient implements AutoCloseable {

        private final ManagedChannel channel;
        private final SubjectServiceClient subjectClient;
        private final AccessBindingClient accessBindingClient;

        public IamClient(IamClientConfiguration config) {
            this.channel = GrpcUtils.newGrpcChannel(config.getAddress(), LzyAuthenticateServiceGrpc.SERVICE_NAME);
            var iamToken = config.createRenewableToken();
            this.subjectClient = new SubjectServiceGrpcClient("TestClient", channel, iamToken::get);
            this.accessBindingClient = new AccessBindingServiceGrpcClient("TestABClient", channel, iamToken::get);
        }

        public ApiTest.User createUser(String name) throws Exception {
            var login = "github-" + name;
            var creds = generateCredentials(login, "GITHUB");

            var subj = subjectClient.createSubject(AuthProvider.GITHUB, login, SubjectType.USER,
                new SubjectCredentials("main", creds.publicKey(), CredentialsType.PUBLIC_KEY));

            return new ApiTest.User(subj.id(), creds.credentials());
        }

        public void addWorkflowAccess(User user, String workflowName) throws Exception {
            final var subj = subjectClient.getSubject(user.id);

            accessBindingClient.setAccessBindings(new Workflow(user.id + "/" + workflowName),
                List.of(new AccessBinding(Role.LZY_WORKFLOW_OWNER, subj)));
        }

        private ApiTest.IamClient.GeneratedCredentials generateCredentials(String login, String provider)
            throws IOException, InterruptedException, NoSuchAlgorithmException, InvalidKeySpecException
        {
            final var keys = RsaUtils.generateRsaKeys();
            var from = Date.from(Instant.now());
            var till = JwtUtils.afterDays(7);
            var credentials = new JwtCredentials(JwtUtils.buildJWT(login, provider, from, till,
                CredentialsUtils.readPrivateKey(keys.privateKey())));

            final var publicKey = keys.publicKey();

            return new ApiTest.IamClient.GeneratedCredentials(publicKey, credentials);
        }

        @Override
        public void close() {
            channel.shutdown();
        }

        public record GeneratedCredentials(
            String publicKey,
            JwtCredentials credentials
        ) {}

    }
}
