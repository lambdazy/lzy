package ai.lzy.portal;

import ai.lzy.allocator.AllocatorAgent;
import ai.lzy.channelmanager.test.BaseTestWithChannelManager;
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
import ai.lzy.model.db.test.DatabaseTestUtils;
import ai.lzy.model.grpc.ProtoConverter;
import ai.lzy.model.slot.SlotInstance;
import ai.lzy.portal.config.PortalConfig;
import ai.lzy.portal.mocks.MocksServer;
import ai.lzy.test.GrpcUtils;
import ai.lzy.util.auth.credentials.CredentialsUtils;
import ai.lzy.util.auth.credentials.JwtCredentials;
import ai.lzy.util.auth.credentials.JwtUtils;
import ai.lzy.util.auth.credentials.RsaUtils;
import ai.lzy.util.grpc.JsonUtils;
import ai.lzy.v1.channel.LzyChannelManagerPrivateGrpc;
import ai.lzy.v1.common.LMO;
import ai.lzy.v1.common.LMS;
import ai.lzy.v1.iam.LzyAuthenticateServiceGrpc;
import ai.lzy.v1.portal.LzyPortal;
import ai.lzy.v1.portal.LzyPortalApi;
import ai.lzy.v1.portal.LzyPortalApi.PortalStatusRequest;
import ai.lzy.v1.portal.LzyPortalGrpc;
import ai.lzy.v1.slots.LSA;
import ai.lzy.v1.slots.LzySlotsApiGrpc;
import ai.lzy.worker.Worker;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import io.findify.s3mock.S3Mock;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.micronaut.context.ApplicationContext;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.locks.LockSupport;

import static ai.lzy.channelmanager.ProtoConverter.makeChannelStatusCommand;
import static ai.lzy.channelmanager.ProtoConverter.makeCreateChannelCommand;
import static ai.lzy.channelmanager.ProtoConverter.makeDestroyChannelCommand;
import static ai.lzy.model.db.test.DatabaseTestUtils.preparePostgresConfig;
import static ai.lzy.util.grpc.GrpcUtils.NO_AUTH_TOKEN;
import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;

public class PortalTestBase {
    private static final Logger LOG = LogManager.getLogger(PortalTestBase.class);

    private static final BaseTestWithIam iamTestContext = new BaseTestWithIam();
    private static final BaseTestWithChannelManager channelManagerTestContext = new BaseTestWithChannelManager();

    private static final int S3_PORT = 8001;
    protected static final String S3_ADDRESS = "http://localhost:" + S3_PORT;
    protected static final String BUCKET_NAME = "lzy-bucket";

    @Rule
    public PreparedDbRule iamDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});
    @Rule
    public PreparedDbRule channelManagerDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});

    private final ApplicationContext context = ApplicationContext.run("test");
    private PortalConfig config;
    private App portal;
    private S3Mock s3;
    private String userId;
    private String workflowName;

    protected MocksServer mocksServer;
    private Map<String, Worker> workers;

    private ManagedChannel portalApiChannel;
    private ManagedChannel portalSlotsChannel;
    protected LzyPortalGrpc.LzyPortalBlockingStub unauthorizedPortalClient;
    private LzyPortalGrpc.LzyPortalBlockingStub authorizedPortalClient;
    private LzySlotsApiGrpc.LzySlotsApiBlockingStub portalSlotsClient;

    private LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateBlockingStub channelManagerPrivateClient;
    private Map<String, String> createdChannels;

    @Before
    public void before() throws Exception {
        System.err.println("---> " + ForkJoinPool.commonPool().getParallelism());

        var iamDbConfig = DatabaseTestUtils.preparePostgresConfig("iam", iamDb.getConnectionInfo());
        iamTestContext.setUp(iamDbConfig);

        var channelManagerDbConfig = preparePostgresConfig("channel-manager", channelManagerDb.getConnectionInfo());
        channelManagerTestContext.setUp(channelManagerDbConfig);
        channelManagerPrivateClient = channelManagerTestContext.getOrCreatePrivateClient(
            iamTestContext.getClientConfig().createRenewableToken());

        createdChannels = new HashMap<>();
        workers = new HashMap<>();

        config = context.getBean(PortalConfig.class);
        var mocksPort = GrpcUtils.rollPort();
        var mocksAddress = "localhost:" + mocksPort;
        config.setIamAddress("localhost:" + iamTestContext.getPort());
        config.setChannelManagerAddress(channelManagerTestContext.getAddress());
        config.setAllocatorAddress(mocksAddress);
        config.setWhiteboardAddress(mocksAddress);
        config.setPortalApiPort(GrpcUtils.rollPort());
        config.setSlotsApiPort(GrpcUtils.rollPort());

        mocksServer = new MocksServer(mocksPort);
        mocksServer.start();

        userId = "uid";
        workflowName = "wf";

        try (final var iamClient = new IamClient(iamTestContext.getClientConfig())) {
            var user = iamClient.createUser(config.getPortalId());
            iamClient.addWorkflowAccess(user.id(), userId, workflowName);
            config.setIamPrivateKey(user.credentials().privateKey());
        }

        startS3();
        startPortal();
    }

    @After
    public void after() throws InterruptedException {
        destroyChannel("portal:stdout");
        destroyChannel("portal:stderr");

        portalApiChannel.shutdown();
        portalSlotsChannel.shutdown();

        stopS3();

        mocksServer.stop();
        mocksServer = null;
        workers = null;

        portal.stop();
        iamTestContext.after();
        System.err.println("CLOSE");
        channelManagerTestContext.after();
    }

    private void startS3() {
        s3 = new S3Mock.Builder().withPort(S3_PORT).withInMemoryBackend().build();
        s3.start();
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
            .withPathStyleAccessEnabled(true)
            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(S3_ADDRESS, "us-west-2"))
            .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
            .build();
        s3Client.createBucket(BUCKET_NAME);
    }

    protected void stopS3() {
        if (Objects.nonNull(s3)) {
            s3.shutdown();
            s3 = null;
        }
    }

    private void startPortal() throws IOException, AllocatorAgent.RegisterException {
        var stdoutChannelId = createChannel("portal:stdout");
        var stderrChannelId = createChannel("portal:stderr");

        config.setStdoutChannelId(stdoutChannelId);
        config.setStderrChannelId(stderrChannelId);

        portal = new App(context);
        portal.start();

        var internalUserCredentials = iamTestContext.getClientConfig().createRenewableToken();

        portalApiChannel = newGrpcChannel("localhost", config.getPortalApiPort(), LzyPortalGrpc.SERVICE_NAME);
        unauthorizedPortalClient = newBlockingClient(
            LzyPortalGrpc.newBlockingStub(portalApiChannel),
            "Test",
            NO_AUTH_TOKEN);

        authorizedPortalClient = newBlockingClient(unauthorizedPortalClient, "TestClient",
            () -> internalUserCredentials.get().token());

        portalSlotsChannel = newGrpcChannel("localhost", config.getSlotsApiPort(), LzySlotsApiGrpc.SERVICE_NAME);
        portalSlotsClient = newBlockingClient(
            LzySlotsApiGrpc.newBlockingStub(portalSlotsChannel),
            "Test",
            NO_AUTH_TOKEN); // TODO: Auth
    }

    protected String prepareTask(int taskNum, boolean newWorker, boolean isInput, String snapshotId) {
        String taskId = "task_" + taskNum;

        String worker = null;
        if (newWorker) {
            worker = "worker_" + taskNum;
            startWorker(worker);
        }

        String channelName = "channel_" + taskNum;
        String channelId = createChannel(channelName);

        String stdoutChannelName = taskId + ":stdout";
        String stdoutChannelId = createChannel(stdoutChannelName);

        String stderrChannelName = taskId + ":stderr";
        String stderrChannelId = createChannel(stderrChannelName);

        String slotName = "/portal_slot_" + taskNum;
        LMS.Slot slot = isInput ? GrpcUtils.makeInputFileSlot(slotName) : GrpcUtils.makeOutputFileSlot(slotName);

        openPortalSlots(LzyPortalApi.OpenSlotsRequest.newBuilder()
            .addSlots(LzyPortal.PortalSlotDesc.newBuilder()
                .setSnapshot(GrpcUtils.makeAmazonSnapshot(snapshotId, BUCKET_NAME, S3_ADDRESS))
                .setSlot(slot)
                .setChannelId(channelId)
                .build())
            .addSlots(LzyPortal.PortalSlotDesc.newBuilder()
                .setSlot(GrpcUtils.makeInputFileSlot("/portal_%s:stdout".formatted(taskId)))
                .setChannelId(stdoutChannelId)
                .setStdout(GrpcUtils.makeStdoutStorage(taskId))
                .build())
            .addSlots(LzyPortal.PortalSlotDesc.newBuilder()
                .setSlot(GrpcUtils.makeInputFileSlot("/portal_%s:stderr".formatted(taskId)))
                .setChannelId(stderrChannelId)
                .setStderr(GrpcUtils.makeStderrStorage(taskId))
                .build())
            .build());

        return worker;
    }

    protected String startTask(int taskNum, String fuze, LMS.Slot slot, String specifiedWorker) {
        String taskId = "task_" + taskNum;
        String actualWorker = Objects.isNull(specifiedWorker) ? "worker_" + taskNum : specifiedWorker;
        String channelId = createdChannels.get("channel_" + taskNum);
        String stdoutChannelId = createdChannels.get(taskId + ":stdout");
        String stderrChannelId = createdChannels.get(taskId + ":stderr");

        mocksServer.getSchedulerMock().startWorker(actualWorker,
            LMO.TaskDesc.newBuilder()
                .setOperation(LMO.Operation.newBuilder()
                    .setName("zygote_" + taskNum)
                    .setCommand(fuze)
                    .setStdout(LMO.Operation.StdSlotDesc.newBuilder()
                        .setName("/dev/stdout")
                        .setChannelId(stdoutChannelId)
                        .build())
                    .setStderr(LMO.Operation.StdSlotDesc.newBuilder()
                        .setName("/dev/stderr")
                        .setChannelId(stderrChannelId)
                        .build())
                    .addSlots(slot)
                    .build())
                .addSlotAssignments(LMO.SlotToChannelAssignment.newBuilder()
                    .setSlotName(slot.getName())
                    .setChannelId(channelId)
                    .build())
                .addSlotAssignments(LMO.SlotToChannelAssignment.newBuilder()
                    .setSlotName("/dev/stdout")
                    .setChannelId(stdoutChannelId)
                    .build())
                .addSlotAssignments(LMO.SlotToChannelAssignment.newBuilder()
                    .setSlotName("/dev/stderr")
                    .setChannelId(stderrChannelId)
                    .build())
                .build(), taskId, "execution-id");

        return taskId;
    }

    protected void startWorker(String workerId) {
        var allocatorDuration = Duration.ofSeconds(5);
        var schedulerDuration = Duration.ofSeconds(1);
        String privateKey;

        try (final var iamClient = new IamClient(iamTestContext.getClientConfig())) {
            var user = iamClient.createUser(workerId);
            workflowName = "wf";
            privateKey = user.credentials().privateKey();
            iamClient.addWorkflowAccess(user.id(), userId, workflowName);
        } catch (Exception e) {
            Assert.fail("Failed to create worker user: " + e.getMessage());
            throw new RuntimeException(e);
        }

        var worker = new Worker(workflowName, workerId, UUID.randomUUID().toString(), config.getAllocatorAddress(),
            config.getAllocatorAddress(), allocatorDuration, schedulerDuration,
            GrpcUtils.rollPort(), GrpcUtils.rollPort(), "/tmp/lzy_" + workerId + "/",
            config.getChannelManagerAddress(), "localhost", privateKey, "token_" + workerId);
        workers.put(workerId, worker);
    }

    protected void waitPortalCompleted() {
        boolean done = false;
        while (!done) {
            var status = authorizedPortalClient.status(PortalStatusRequest.newBuilder().build());
            done = status.getSlotsList().stream().allMatch(
                slot -> {
                    System.out.println("[portal slot] " + JsonUtils.printSingleLine(slot));
                    return switch (slot.getSlot().getDirection()) {
                        case INPUT -> Set.of(LMS.SlotStatus.State.UNBOUND, LMS.SlotStatus.State.OPEN,
                            LMS.SlotStatus.State.DESTROYED).contains(slot.getState());
                        case OUTPUT -> true;
                        case UNKNOWN, UNRECOGNIZED -> throw new RuntimeException("Unexpected state");
                    };
                });
            if (!done) {
                LockSupport.parkNanos(Duration.ofMillis(100).toNanos());
            }
        }
    }

    protected String createChannel(String name) {
        final var response = channelManagerPrivateClient.create(
            makeCreateChannelCommand(userId, workflowName, UUID.randomUUID().toString(), name));
        System.out.println("Channel '" + name + "' created: " + response.getChannelId());
        createdChannels.put(name, response.getChannelId());
        return response.getChannelId();
    }

    protected void destroyChannel(String name) {
        String id = createdChannels.get(name);
        if (id != null) {
            channelManagerPrivateClient.destroy(makeDestroyChannelCommand(id));
            System.out.println("Channel '" + name + "' removed");
            createdChannels.remove(name);
        }
    }

    protected void openPortalSlots(LzyPortalApi.OpenSlotsRequest request) {
        var response = authorizedPortalClient.openSlots(request);
        Assert.assertTrue(response.getDescription(), response.getSuccess());
    }

    protected Status openPortalSlotsWithFail(LzyPortalApi.OpenSlotsRequest request) {
        Status status = null;
        try {
            var response = authorizedPortalClient.openSlots(request);
            Assert.fail(response.getDescription());
        } catch (StatusRuntimeException e) {
            status = e.getStatus();
        }
        return status;
    }

    protected Iterator<LSA.SlotDataChunk> openOutputSlot(SlotInstance slot) {
        return portalSlotsClient.openOutputSlot(
            LSA.SlotDataRequest.newBuilder()
                .setSlotInstance(ProtoConverter.toProto(slot))
                .setOffset(0)
                .build());
    }

    protected ArrayBlockingQueue<Object> readPortalSlot(String channelName) {
        String channelId = createdChannels.get(channelName);
        LMS.SlotInstance portalSlot = null;

        int n = 100;
        while (n-- > 0) {
            try {
                var status = channelManagerPrivateClient.status(makeChannelStatusCommand(channelId));
                portalSlot = status.getStatus().getChannel().getSenders().getPortalSlot();
            } catch (StatusRuntimeException e) {
                if (e.getStatus().getCode() == Status.NOT_FOUND.getCode()) {
                    portalSlot = null;
                }
            }
            if (portalSlot != null) {
                break;
            }
            LockSupport.parkNanos(Duration.ofMillis(10).toNanos());
        }

        Assert.assertNotNull(portalSlot);

        var iter = openOutputSlot(ProtoConverter.fromProto(portalSlot));

        var values = new ArrayBlockingQueue<>(100);

        ForkJoinPool.commonPool().execute(() -> {
            try {
                iter.forEachRemaining(message -> {
                    System.out.println(" ::: got " + JsonUtils.printSingleLine(message));
                    switch (message.getKindCase()) {
                        case CONTROL -> {
                            if (LSA.SlotDataChunk.Control.EOS != message.getControl()) {
                                values.offer(new AssertionError(JsonUtils.printSingleLine(message)));
                            }
                        }
                        case CHUNK -> values.offer(message.getChunk().toStringUtf8());
                        default -> values.offer(new AssertionError(JsonUtils.printSingleLine(message)));
                    }
                });
            } catch (Exception e) {
                values.offer(e);
            }
        });

        return values;
    }

    public record User(
        String id,
        IamClient.GeneratedCredentials credentials
    ) { }

    public static class IamClient implements AutoCloseable {

        private final ManagedChannel channel;
        private final SubjectServiceClient subjectClient;
        private final AccessBindingClient accessBindingClient;

        IamClient(IamClientConfiguration config) {
            this.channel = newGrpcChannel(config.getAddress(), LzyAuthenticateServiceGrpc.SERVICE_NAME);
            var iamToken = config.createRenewableToken();
            this.subjectClient = new SubjectServiceGrpcClient("TestClient", channel, iamToken::get);
            this.accessBindingClient = new AccessBindingServiceGrpcClient("TestABClient", channel, iamToken::get);
        }

        public User createUser(String portalId) throws Exception {
            var creds = generateCredentials(portalId, "INTERNAL");

            var subj = subjectClient.createSubject(AuthProvider.INTERNAL, portalId, SubjectType.WORKER,
                new SubjectCredentials("main", creds.publicKey(), CredentialsType.PUBLIC_KEY));

            return new User(subj.id(), creds);
        }

        public void addWorkflowAccess(String subjId, String userId, String workflowName) {
            final var subj = subjectClient.getSubject(subjId);

            accessBindingClient.setAccessBindings(new Workflow(userId + "/" + workflowName),
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

            return new GeneratedCredentials(publicKey, keys.privateKey(), credentials);
        }

        @Override
        public void close() {
            channel.shutdown();
        }

        public record GeneratedCredentials(
            String publicKey,
            String privateKey,
            JwtCredentials credentials
        ) { }

    }
}
