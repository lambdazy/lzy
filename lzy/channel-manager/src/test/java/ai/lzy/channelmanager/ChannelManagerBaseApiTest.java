package ai.lzy.channelmanager;

import ai.lzy.channelmanager.v2.ChannelManagerApp;
import ai.lzy.channelmanager.v2.config.ChannelManagerConfig;
import ai.lzy.channelmanager.v2.dao.ChannelManagerDataSource;
import ai.lzy.channelmanager.v2.debug.InjectedFailures;
import ai.lzy.channelmanager.v2.operation.ChannelOperationManager;
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
import ai.lzy.iam.test.BaseTestWithIam;
import ai.lzy.longrunning.OperationUtils;
import ai.lzy.model.DataScheme;
import ai.lzy.model.db.test.DatabaseTestUtils;
import ai.lzy.util.auth.credentials.CredentialsUtils;
import ai.lzy.util.auth.credentials.JwtCredentials;
import ai.lzy.util.auth.credentials.JwtUtils;
import ai.lzy.util.auth.credentials.RsaUtils;
import ai.lzy.util.grpc.GrpcUtils;
import ai.lzy.v1.channel.v2.LCM;
import ai.lzy.v1.channel.v2.LCM.Channel;
import ai.lzy.v1.channel.v2.LCM.ChannelSpec;
import ai.lzy.v1.channel.v2.LCMPS.ChannelCreateRequest;
import ai.lzy.v1.channel.v2.LCMPS.ChannelDestroyAllRequest;
import ai.lzy.v1.channel.v2.LCMPS.ChannelDestroyRequest;
import ai.lzy.v1.channel.v2.LCMPS.ChannelStatusAllRequest;
import ai.lzy.v1.channel.v2.LCMPS.ChannelStatusRequest;
import ai.lzy.v1.channel.v2.LCMS.BindRequest;
import ai.lzy.v1.channel.v2.LCMS.UnbindRequest;
import ai.lzy.v1.channel.v2.LzyChannelManagerGrpc;
import ai.lzy.v1.channel.v2.LzyChannelManagerPrivateGrpc;
import ai.lzy.v1.common.LMS;
import ai.lzy.v1.iam.LzyAuthenticateServiceGrpc;
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
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static ai.lzy.model.UriScheme.LzyFs;
import static ai.lzy.model.db.test.DatabaseTestUtils.preparePostgresConfig;
import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcServer;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ChannelManagerBaseApiTest {
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
    protected User user;
    protected String workflowName;

    private Server mockedSlotApiServer;
    private HostAndPort mockedSlotApiAddress;

    @Before
    public void before() throws Exception {
        var iamDbConfig = preparePostgresConfig("iam", iamDb.getConnectionInfo());
        iamTestContext.setUp(iamDbConfig);

        var channelManagerDbConfig = preparePostgresConfig("channel-manager", channelManagerDb.getConnectionInfo());
        context = ApplicationContext.run(channelManagerDbConfig);
        app = context.getBean(ChannelManagerApp.class);
        app.start();

        config = context.getBean(ChannelManagerConfig.class);
        channel = newGrpcChannel(config.getAddress(), LzyChannelManagerPrivateGrpc.SERVICE_NAME);

        workflowName = "wfName";
        try (final var iamClient = new IamClient(config.getIam())) {
            user = iamClient.createUser("workflowUser");
            iamClient.addWorkflowAccess(user, workflowName);
        }

        var internalUserCredentials = config.getIam().createRenewableToken();
        privateClient = newBlockingClient(LzyChannelManagerPrivateGrpc.newBlockingStub(channel),
            "AuthPrivateTest", () -> internalUserCredentials.get().token());

        publicClient = newBlockingClient(LzyChannelManagerGrpc.newBlockingStub(channel),
            "AuthPublicTest", () -> user.credentials().token());

        operationApiClient = newBlockingClient(LongRunningServiceGrpc.newBlockingStub(channel),
            "OpTest", () -> internalUserCredentials.get().token());

        mockedSlotApiAddress = HostAndPort.fromString(config.getStubSlotApiAddress());
        var slotService = new SlotServiceMock();
        mockedSlotApiServer = newGrpcServer(mockedSlotApiAddress, null)
            .addService(slotService.slotApiService())
            .addService(slotService.operationService())
            .build();
        mockedSlotApiServer.start();

        InjectedFailures.clear();
    }

    @After
    public void after() throws InterruptedException {
        InjectedFailures.assertClean();

        iamTestContext.after();
        app.stop();
        app.awaitTermination();
        mockedSlotApiServer.shutdown();
        mockedSlotApiServer.awaitTermination();
        channel.shutdown();
        channel.awaitTermination(10, TimeUnit.SECONDS);
        DatabaseTestUtils.cleanup(context.getBean(ChannelManagerDataSource.class));
        context.close();
    }

    protected ChannelCreateRequest makeChannelCreateCommand(String executionId, String channelName) {
        return ChannelCreateRequest.newBuilder()
            .setUserId(user.id)
            .setWorkflowName(workflowName)
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

    protected ChannelStatusAllRequest makeChannelStatusAllCommand(String executionId) {
        return ChannelStatusAllRequest.newBuilder()
            .setExecutionId(executionId)
            .build();
    }

    protected BindRequest makeBindCommand(String channelId, String slotName,
                                          LMS.Slot.Direction slotDirection, BindRequest.SlotOwner slotOwner)
    {
        return makeBindCommand(channelId, "tid", slotName, slotDirection, slotOwner);
    }

    protected BindRequest makeBindCommand(String channelId, String taskId, String slotName,
                                          LMS.Slot.Direction slotDirection, BindRequest.SlotOwner slotOwner)
    {
        URI slotUri = URI.create("%s://%s:%d".formatted(LzyFs.scheme(),
            mockedSlotApiAddress.getHost(), mockedSlotApiAddress.getPort()
        )).resolve(Path.of("/", taskId, slotName).toString());

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
            operationApiClient, operationId, Duration.ofSeconds(10));
        assertTrue(operation.hasResponse());
        return operation;
    }

    protected LCM.Channel prepareChannelOfShape(String executionId, String channelName,
                                                int expectedPortalSenders, int expectedWorkerSenders,
                                                int expectedPortalReceivers, int expectedWorkerReceivers)
    {
        var channelCreateResponse = privateClient.create(makeChannelCreateCommand(executionId, channelName));
        String channelId = channelCreateResponse.getChannelId();

        List<LongRunning.Operation> operations = new ArrayList<>();
        if (expectedPortalReceivers != 0) {
            assertEquals(1, expectedPortalReceivers);
            var op = publicClient.bind(makeBindCommand(channelId, "tid-" + channelName, "inSlotP",
                LMS.Slot.Direction.INPUT, BindRequest.SlotOwner.PORTAL));
            operations.add(op);
        }
        if (expectedWorkerReceivers != 0) {
            for (int i = 1; i <= expectedWorkerReceivers; ++i) {
                var op = publicClient.bind(makeBindCommand(channelId, "tid-" + channelName, "inSlotW" + i,
                    LMS.Slot.Direction.INPUT, BindRequest.SlotOwner.WORKER));
                operations.add(op);
            }
        }
        if (expectedPortalSenders != 0) {
            assertEquals(1, expectedPortalSenders);
            var op = publicClient.bind(makeBindCommand(channelId, "tid-" + channelName, "outSlotP",
                LMS.Slot.Direction.OUTPUT, BindRequest.SlotOwner.PORTAL));
            operations.add(op);
        }
        if (expectedWorkerSenders != 0) {
            assertEquals(1, expectedWorkerSenders);
            var op = publicClient.bind(makeBindCommand(channelId, "tid-" + channelName, "outSlotW",
                LMS.Slot.Direction.OUTPUT, BindRequest.SlotOwner.WORKER));
            operations.add(op);
        }

        operations.forEach(op -> awaitOperationResponse(op.getId()));

        var status = privateClient.status(makeChannelStatusCommand(channelId));
        assertChannelShape(
            expectedPortalSenders, expectedWorkerSenders,
            expectedPortalReceivers, expectedWorkerReceivers,
            status.getStatus().getChannel());

        return status.getStatus().getChannel();
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

    protected void restoreActiveOperations() {
        var manager = context.getBean(ChannelOperationManager.class);
        manager.restoreActiveOperations();
    }

    public record User(
        String id,
        JwtCredentials credentials
    ) { }

    public static class IamClient implements AutoCloseable {

        private final ManagedChannel channel;
        private final SubjectServiceClient subjectClient;
        private final AccessBindingClient accessBindingClient;

        IamClient(IamClientConfiguration config) {
            this.channel = GrpcUtils.newGrpcChannel(config.getAddress(), LzyAuthenticateServiceGrpc.SERVICE_NAME);
            var iamToken = config.createRenewableToken();
            this.subjectClient = new SubjectServiceGrpcClient("TestClient", channel, iamToken::get);
            this.accessBindingClient = new AccessBindingServiceGrpcClient("TestABClient", channel, iamToken::get);
        }

        public User createUser(String name) throws Exception {
            var login = "github-" + name;
            var creds = generateCredentials(login, "GITHUB");

            var subj = subjectClient.createSubject(AuthProvider.GITHUB, login, SubjectType.USER,
                new SubjectCredentials("main", creds.publicKey(), CredentialsType.PUBLIC_KEY));

            return new User(subj.id(), creds.credentials());
        }

        public void addWorkflowAccess(User user, String workflowName) throws Exception {
            final var subj = subjectClient.getSubject(user.id);

            accessBindingClient.setAccessBindings(new Workflow(user.id + "/" + workflowName),
                List.of(new AccessBinding(Role.LZY_WORKFLOW_OWNER, subj)));
        }

        private GeneratedCredentials generateCredentials(String login, String provider)
            throws IOException, InterruptedException, NoSuchAlgorithmException, InvalidKeySpecException
        {
            final var keys = RsaUtils.generateRsaKeys();
            var from = Date.from(Instant.now());
            var till = JwtUtils.afterDays(7);
            var credentials = new JwtCredentials(JwtUtils.buildJWT(login, provider, from, till,
                CredentialsUtils.readPrivateKey(keys.privateKey())));

            final var publicKey = keys.publicKey();

            return new GeneratedCredentials(publicKey, credentials);
        }

        @Override
        public void close() {
            channel.shutdown();
        }

        public record GeneratedCredentials(
            String publicKey,
            JwtCredentials credentials
        ) { }

    }

}

