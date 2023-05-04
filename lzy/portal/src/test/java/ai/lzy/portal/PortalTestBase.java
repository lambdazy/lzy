package ai.lzy.portal;

import ai.lzy.allocator.AllocatorAgent;
import ai.lzy.channelmanager.test.BaseTestWithChannelManager;
import ai.lzy.common.IdGenerator;
import ai.lzy.common.RandomIdGenerator;
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
import ai.lzy.util.kafka.KafkaAdminClient;
import ai.lzy.util.kafka.KafkaConfig;
import ai.lzy.util.kafka.KafkaHelper;
import ai.lzy.util.kafka.ScramKafkaAdminClient;
import ai.lzy.v1.channel.LzyChannelManagerPrivateGrpc;
import ai.lzy.v1.common.LME;
import ai.lzy.v1.common.LMO;
import ai.lzy.v1.common.LMS;
import ai.lzy.v1.iam.LzyAuthenticateServiceGrpc;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import ai.lzy.v1.portal.LzyPortal;
import ai.lzy.v1.portal.LzyPortalApi;
import ai.lzy.v1.portal.LzyPortalApi.PortalStatusRequest;
import ai.lzy.v1.portal.LzyPortalGrpc;
import ai.lzy.v1.slots.LSA;
import ai.lzy.v1.slots.LzySlotsApiGrpc;
import ai.lzy.v1.worker.LWS;
import ai.lzy.v1.worker.WorkerApiGrpc;
import ai.lzy.worker.ServiceConfig;
import ai.lzy.worker.Worker;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import io.findify.s3mock.S3Mock;
import io.github.embeddedkafka.EmbeddedK;
import io.github.embeddedkafka.EmbeddedKafka;
import io.github.embeddedkafka.EmbeddedKafkaConfig$;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.micronaut.context.ApplicationContext;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.*;
import org.junit.rules.Timeout;
import scala.collection.immutable.Map$;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;

import static ai.lzy.channelmanager.ProtoConverter.makeChannelStatusCommand;
import static ai.lzy.channelmanager.ProtoConverter.makeCreateChannelCommand;
import static ai.lzy.channelmanager.ProtoConverter.makeDestroyChannelCommand;
import static ai.lzy.longrunning.OperationUtils.awaitOperationDone;
import static ai.lzy.model.db.test.DatabaseTestUtils.preparePostgresConfig;
import static ai.lzy.util.grpc.GrpcUtils.NO_AUTH_TOKEN;
import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;
import static org.junit.Assert.assertTrue;

public class PortalTestBase {
    private static final Logger LOG = LogManager.getLogger(PortalTestBase.class);

    @Rule
    public Timeout globalTimeout = Timeout.seconds(60);

    private static final BaseTestWithIam iamTestContext = new BaseTestWithIam();
    private final BaseTestWithChannelManager channelManagerTestContext = new BaseTestWithChannelManager();

    private static final int S3_PORT = 8001;
    protected static final String S3_ADDRESS = "http://localhost:" + S3_PORT;
    protected static final String BUCKET_NAME = "lzybucket";

    @ClassRule
    public static PreparedDbRule iamDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});
    private static String mocksAddress;

    @Rule
    public PreparedDbRule channelManagerDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});

    private ApplicationContext context;
    private PortalConfig config;
    private static App portal;
    private static S3Mock s3;
    private static String userId;
    private static String workflowName;
    private static String executionId;

    protected static MocksServer mocksServer;
    private static final AtomicReference<WorkerDesc> worker = new AtomicReference<>(null);
    private static ManagedChannel portalApiChannel;
    private static ManagedChannel portalSlotsChannel;
    protected static LzyPortalGrpc.LzyPortalBlockingStub unauthorizedPortalClient;
    private static LzyPortalGrpc.LzyPortalBlockingStub authorizedPortalClient;
    private static LzySlotsApiGrpc.LzySlotsApiBlockingStub portalSlotsClient;
    private static LongRunningServiceGrpc.LongRunningServiceBlockingStub portalOpsClient;

    private static LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateBlockingStub channelManagerPrivateClient;
    private static Map<String, String> createdChannels;

    protected static String kafkaBootstrapServer;
    private static EmbeddedK kafka;
    protected static KafkaAdminClient kafkaAdminClient;

    protected static final IdGenerator idGenerator = new RandomIdGenerator();

    protected ReadKafkaTopicFinisher finishStdlogsReader;
    protected LMO.KafkaTopicDescription stdlogsTopic;
    protected ArrayBlockingQueue<Object> stdlogs;

    static {
        Worker.selectRandomValues(true);
    }

    private static User portalUser;

    @BeforeClass
    public static void beforeTest() throws Exception {
        var iamDbConfig = DatabaseTestUtils.preparePostgresConfig("iam", iamDb.getConnectionInfo());
        iamTestContext.setUp(iamDbConfig);
        createdChannels = new HashMap<>();

        var mocksPort = GrpcUtils.rollPort();
        mocksAddress = "localhost:" + mocksPort;

        mocksServer = new MocksServer(mocksPort);
        mocksServer.start();

        var kafkaPort = GrpcUtils.rollPort();
        var zkPort = GrpcUtils.rollPort();
        kafkaBootstrapServer = "localhost:" + kafkaPort;
        @SuppressWarnings("unchecked")
        var brokerConf = (scala.collection.immutable.Map<String, String>) Map$.MODULE$.<String, String>empty()
            .updated("zookeeper.session.timeout.ms", "60000")
            .updated("zookeeper.connection.timeout.ms", "60000");
        var conf = Map$.MODULE$.<String, String>empty();
        var config = EmbeddedKafkaConfig$.MODULE$.apply(kafkaPort, zkPort, brokerConf, conf, conf);
        kafka = EmbeddedKafka.start(config);
        KafkaHelper.USE_AUTH.set(false);
        kafkaAdminClient = new ScramKafkaAdminClient(KafkaConfig.of(kafkaBootstrapServer));

        userId = "uid";
        workflowName = "wf";
        executionId = "exec";
    }

    @AfterClass
    public static void afterTest() throws InterruptedException {
        if (worker.get() != null) {
            worker.get().worker().stop();
        }

        mocksServer.stop();
        mocksServer = null;

        kafkaAdminClient.shutdown();
        kafka.stop(true);
        KafkaHelper.USE_AUTH.set(true);

        iamTestContext.after();
    }

    @Before
    public void before() throws Exception {
        context = ApplicationContext.run("test");
        config = context.getBean(PortalConfig.class);

        try (final var iamClient = new IamClient(iamTestContext.getClientConfig())) {
            var user = iamClient.createUser(config.getPortalId());
            portalUser = user;
            iamClient.addWorkflowAccess(user.id(), userId, workflowName);
        } catch (Exception e) {
            LOG.error(e);
        }

        config.setIamPrivateKey(portalUser.credentials().privateKey());

        var channelManagerCfgOverrides = preparePostgresConfig("channel-manager", channelManagerDb.getConnectionInfo());
        channelManagerCfgOverrides.put("channel-manager.iam.address", "localhost:" + iamTestContext.getPort());
        channelManagerTestContext.setUp(channelManagerCfgOverrides);
        channelManagerPrivateClient = channelManagerTestContext.getOrCreatePrivateClient(
            iamTestContext.getClientConfig().createRenewableToken());

        config.setChannelManagerAddress(channelManagerTestContext.getAddress());
        config.setIamAddress("localhost:" + iamTestContext.getPort());
        config.setAllocatorAddress(mocksAddress);
        config.setWhiteboardAddress(mocksAddress);
        config.setPortalApiPort(GrpcUtils.rollPort());
        config.setSlotsApiPort(GrpcUtils.rollPort());

        startPortal();
        startS3();

        finishStdlogsReader = new ReadKafkaTopicFinisher();
        stdlogsTopic = prepareKafkaTopic("kafkauser", "password", idGenerator.generate("stdlogs-", 5));
        stdlogs = readKafkaTopic(stdlogsTopic.getTopic(), finishStdlogsReader);
    }

    @After
    public void after() throws InterruptedException {
        portalApiChannel.shutdown();
        portalSlotsChannel.shutdown();
        portal.stop();
        channelManagerTestContext.after();

        stopS3();

        finishStdlogsReader.finish();
        dropKafkaTopicSafe(stdlogsTopic.getTopic());
    }

    private static void startS3() {
        s3 = new S3Mock.Builder().withPort(S3_PORT).withInMemoryBackend().build();
        s3.start();
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
            .withPathStyleAccessEnabled(true)
            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(S3_ADDRESS, "us-west-2"))
            .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
            .build();
        s3Client.createBucket(BUCKET_NAME);
    }

    protected static void stopS3() {
        if (Objects.nonNull(s3)) {
            s3.shutdown();
            s3 = null;
        }
    }

    protected static void dropKafkaTopicSafe(String topicName) {
        try {
            kafkaAdminClient.dropTopic(topicName);
        } catch (StatusRuntimeException ignored) {
            // ignore
        }
    }

    protected static LMO.KafkaTopicDescription prepareKafkaTopic(String username, String password, String topicName) {
        kafkaAdminClient.createTopic(topicName);
        try {
            kafkaAdminClient.createUser(username, password);
        } catch (StatusRuntimeException e) {
            if (!e.getStatus().getCode().equals(Status.Code.ALREADY_EXISTS)) {
                throw e;
            }
        }
        //kafkaAdminClient.grantPermission(username, topicName);

        return LMO.KafkaTopicDescription.newBuilder()
            .addBootstrapServers(kafkaBootstrapServer)
            .setTopic(topicName)
            .setUsername(username)
            .setPassword(password)
            .build();
    }

    private void startPortal() throws IOException, AllocatorAgent.RegisterException {
        portal = context.getBean(App.class);
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
            portalUser.credentials().credentials()::token);

        portalOpsClient = newBlockingClient(LongRunningServiceGrpc.newBlockingStub(portalApiChannel), "TestClient",
            () -> internalUserCredentials.get().token());
    }

    protected void finishPortal() {
        var op = authorizedPortalClient.finish(LzyPortalApi.FinishRequest.getDefaultInstance());
        op = awaitOperationDone(portalOpsClient, op.getId(), Duration.ofSeconds(5));
        assertTrue(op.getDone());
        assertTrue(op.hasResponse());
    }

    protected String startTask(String fuze, String workerSlotName, WorkerDesc desc, boolean isPortalInput,
                               String snapshotId, LMO.KafkaTopicDescription stdLogsTopic)
    {
        var uniqId = idGenerator.generate();

        String channelName = "channel_" + uniqId;
        String channelId = createChannel(channelName);

        String slotName = "/portal_slot_" + uniqId;
        LMS.Slot slot = isPortalInput ? GrpcUtils.makeInputFileSlot(slotName) : GrpcUtils.makeOutputFileSlot(slotName);
        var taskSlot = !isPortalInput ? GrpcUtils.makeInputFileSlot(workerSlotName)
            : GrpcUtils.makeOutputFileSlot(workerSlotName);

        openPortalSlots(LzyPortalApi.OpenSlotsRequest.newBuilder()
            .addSlots(LzyPortal.PortalSlotDesc.newBuilder()
                .setSnapshot(GrpcUtils.makeAmazonSnapshot(snapshotId, BUCKET_NAME, S3_ADDRESS))
                .setSlot(slot)
                .setChannelId(channelId)
                .build())
            .build());

        var op = desc.workerStub.execute(LWS.ExecuteRequest.newBuilder()
            .setTaskDesc(LMO.TaskDesc.newBuilder()
                .setOperation(LMO.Operation.newBuilder()
                    .setEnv(LME.EnvSpec.newBuilder()
                        .setProcessEnv(LME.ProcessEnv.newBuilder().build())
                        .build())
                    .setName("zygote_" + uniqId)
                    .setCommand(fuze)
                    .addSlots(taskSlot)
                    .setKafkaTopic(stdLogsTopic)
                    .build())
                .addSlotAssignments(LMO.SlotToChannelAssignment.newBuilder()
                    .setSlotName(taskSlot.getName())
                    .setChannelId(channelId)
                    .build())
                .build())
            .setTaskId(uniqId)
            .setExecutionId(executionId)
            .build());

        while (!op.getDone()) {
            op = desc.opStub.get(LongRunning.GetOperationRequest.newBuilder()
                .setOperationId(op.getId())
                .build());
        }

        return uniqId;
    }

    protected synchronized WorkerDesc startWorker() {
        if (worker.get() != null) {
            var ref = worker.get();
            worker.set(null);
            return ref;
        }

        var workerId = idGenerator.generate("worker-");
        var allocatorDuration = Duration.ofSeconds(5);

        var ctx = Worker.startApplication(workerId,
            config.getAllocatorAddress(), config.getIamAddress(), allocatorDuration,
            config.getChannelManagerAddress(), "localhost", "token-" + workerId, 0,
            KafkaConfig.of(kafkaBootstrapServer));

        var worker = ctx.getBean(Worker.class);
        var config = ctx.getBean(ServiceConfig.class);

        try (final var iamClient = new IamClient(iamTestContext.getClientConfig())) {
            var user = iamClient.createUser(workerId, config.getPublicKey());
            workflowName = "wf";
            iamClient.addWorkflowAccess(user, userId, workflowName);
        } catch (Exception e) {
            Assert.fail("Failed to create worker user: " + e.getMessage());
            throw new RuntimeException(e);
        }
        var workerChannel = ai.lzy.util.grpc.GrpcUtils.newGrpcChannel("localhost:" + config.getApiPort(),
            WorkerApiGrpc.SERVICE_NAME);

        var stub = WorkerApiGrpc.newBlockingStub(workerChannel);

        stub = ai.lzy.util.grpc.GrpcUtils.newBlockingClient(stub, "worker", () -> iamTestContext.getClientConfig()
            .createRenewableToken().get().token());

        var opStub = LongRunningServiceGrpc.newBlockingStub(workerChannel);
        opStub = ai.lzy.util.grpc.GrpcUtils.newBlockingClient(opStub, "worker", () -> iamTestContext.getClientConfig()
            .createRenewableToken().get().token());

        return new WorkerDesc(worker, workerChannel, stub, opStub);
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
                LockSupport.parkNanos(Duration.ofMillis(300).toNanos());
            }
        }
    }

    protected static String createChannel(String name) {
        final var response = channelManagerPrivateClient.create(
            makeCreateChannelCommand(userId, workflowName, executionId, name));
        System.out.println("Channel '" + name + "' created: " + response.getChannelId());
        createdChannels.put(name, response.getChannelId());
        return response.getChannelId();
    }

    protected static void destroyChannel(String name) {
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

        var thread  = new Thread(() -> {
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
                LOG.error("Cannot read portal slot from channel {}: {}", channelName, e.getMessage());
                values.offer(e);
            }
        }, "");
        thread.start();

        return values;
    }

    public record EosMessage(String taskId, String stream) {}

    public record StdlogMessage(String taskId, String stream, String line) {
        public static StdlogMessage out(String taskId, String line) {
            return new StdlogMessage(taskId, "out", line);
        }
        public static StdlogMessage err(String taskId, String line) {
            return new StdlogMessage(taskId, "err", line);
        }
    }

    public static final class ReadKafkaTopicFinisher {
        private volatile boolean finish = false;
        private volatile boolean consumerFinished = false;

        public boolean shouldFinish() {
            return finish;
        }

        public synchronized void finish() {
            System.out.println(" --> finish kafka topic reader...");
            if (finish) {
                return;
            }
            finish = true;
            while (!consumerFinished) {
                LockSupport.parkNanos(Duration.ofMillis(10).toNanos());
            }
        }

        void consumerFinished() {
            consumerFinished = true;
        }
    }

    /**
     * @return Exception on error
     *         EosMessage on EOS
     *         StdlogMessage for each output line
     */
    protected static ArrayBlockingQueue<Object> readKafkaTopic(String topicName, ReadKafkaTopicFinisher finisher) {
        var values = new ArrayBlockingQueue<>(100);

        var props = new KafkaHelper(KafkaConfig.of(kafkaBootstrapServer)).toProperties();
        props.put("group.id", idGenerator.generate("portal-test-"));

        var thread = new Thread(() -> {
            try (var consumer = new KafkaConsumer<String, byte[]>(props)) {
                var partition = new TopicPartition(topicName, /* partition */ 0);

                consumer.assign(List.of(partition));
                consumer.seek(partition, 0);

                var ts = System.currentTimeMillis();

                while (!finisher.shouldFinish()) {
                    var records = consumer.poll(Duration.ofMillis(100));
                    if (records.count() <= 0) {
                        var now = System.currentTimeMillis();
                        if (ts - now > 5000) {
                            System.out.println("... waiting for data at topic " + topicName);
                            ts = now;
                        }
                        continue;
                    }

                    ts = System.currentTimeMillis();

                    // consumer.commitSync();

                    for (var record : records) {
                        var taskId = record.key();
                        var stream = new String(record.headers().lastHeader("stream").value(), StandardCharsets.UTF_8);

                        var eos = record.headers().lastHeader("eos") != null;
                        if (eos) {
                            var msg = new EosMessage(taskId, stream);
                            System.out.println(" ::: got " + msg);
                            values.offer(msg);
                            continue;
                        }

                        var lines = new String(record.value(), StandardCharsets.UTF_8);
                        for (var line : lines.split("\n")) {
                            var msg = new StdlogMessage(taskId, stream, line);
                            System.out.println(" ::: got " + msg);
                            values.offer(msg);
                        }
                    }
                }
            } catch (Exception e) {
                LOG.error("Cannot read from topic {}: {}", topicName, e.getMessage());
                values.offer(e);
            } finally {
                finisher.consumerFinished();
            }
        });
        thread.start();

        return values;
    }

    protected void assertStdLogs(BlockingQueue<Object> logs, List<StdlogMessage> stdout, List<StdlogMessage> stderr)
        throws InterruptedException
    {
        var expectedStdout = new HashMap<String, Queue<StdlogMessage>>();
        var expectedStderr = new HashMap<String, Queue<StdlogMessage>>();
        var eosStdout = new HashSet<String>();
        var eosStderr = new HashSet<String>();

        record TaskStream(String taskId, String stream) {}

        var notFinishedStreams = new HashSet<TaskStream>();

        for (var out : stdout) {
            expectedStdout.computeIfAbsent(out.taskId, __ -> new ArrayDeque<>()).add(out);
            notFinishedStreams.add(new TaskStream(out.taskId, "out"));
            notFinishedStreams.add(new TaskStream(out.taskId, "err"));
        }

        for (var err : stderr) {
            expectedStderr.computeIfAbsent(err.taskId, __ -> new ArrayDeque<>()).add(err);
            notFinishedStreams.add(new TaskStream(err.taskId, "out"));
            notFinishedStreams.add(new TaskStream(err.taskId, "err"));
        }

        System.out.println(" --> waiting for streams: " +
            notFinishedStreams.stream().map(Objects::toString).collect(Collectors.joining(",")));

        while (!notFinishedStreams.isEmpty()) {
            var log = logs.take();
            if (log instanceof StdlogMessage msg) {
                var remains = "out".equals(msg.stream)
                    ? expectedStdout.get(msg.taskId)
                    : expectedStderr.get(msg.taskId);

                Assert.assertNotNull(remains);
                Assert.assertFalse(remains.isEmpty());
                Assert.assertEquals(remains.remove(), msg);
            } else if (log instanceof EosMessage msg) {
                var set = "out".equals(msg.stream) ? eosStdout : eosStderr;
                Assert.assertTrue(set.add(msg.taskId));

                var remains = "out".equals(msg.stream)
                    ? expectedStdout.get(msg.taskId)
                    : expectedStderr.get(msg.taskId);

                Assert.assertTrue(remains == null || remains.isEmpty());
                notFinishedStreams.remove(new TaskStream(msg.taskId, msg.stream));

                System.out.println(" --> waiting for streams: " +
                    notFinishedStreams.stream().map(Objects::toString).collect(Collectors.joining(",")));
            } else {
                Assert.fail(log.toString());
            }
        }
    }

    public record User(
        String id,
        IamClient.GeneratedCredentials credentials
    ) {}

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

        public String createUser(String portalId, String pk) throws Exception {
            var subj = subjectClient.createSubject(AuthProvider.INTERNAL, portalId, SubjectType.WORKER,
                new SubjectCredentials("main", pk, CredentialsType.PUBLIC_KEY));

            return subj.id();
        }

        public void addWorkflowAccess(String subjId, String userId, String workflowName) {
            final var subj = subjectClient.getSubject(subjId);

            accessBindingClient.setAccessBindings(new Workflow(userId + "/" + workflowName),
                List.of(new AccessBinding(Role.LZY_WORKER, subj)));
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
        ) {}

    }

    public record WorkerDesc(
        Worker worker,
        ManagedChannel channel,
        WorkerApiGrpc.WorkerApiBlockingStub workerStub,
        LongRunningServiceGrpc.LongRunningServiceBlockingStub opStub
    ) implements AutoCloseable
    {

        @Override
        public void close() {
            if (PortalTestBase.worker.get() == null) {
                PortalTestBase.worker.set(this);
                return;
            }
            worker.stop();
            channel.shutdownNow();
            try {
                channel.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
