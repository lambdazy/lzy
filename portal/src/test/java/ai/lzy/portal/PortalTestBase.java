package ai.lzy.portal;

import ai.lzy.allocator.AllocatorAgent;
import ai.lzy.channelmanager.grpc.ChannelManagerMock;
import ai.lzy.iam.test.BaseTestWithIam;
import ai.lzy.model.db.test.DatabaseTestUtils;
import ai.lzy.model.grpc.ProtoConverter;
import ai.lzy.model.slot.SlotInstance;
import ai.lzy.portal.config.PortalConfig;
import ai.lzy.servant.agents.Worker;
import ai.lzy.test.GrpcUtils;
import ai.lzy.util.auth.credentials.RsaUtils;
import ai.lzy.util.grpc.JsonUtils;
import ai.lzy.v1.common.LMO;
import ai.lzy.v1.common.LMS;
import ai.lzy.v1.fs.LzyFsGrpc;
import ai.lzy.v1.portal.LzyPortal;
import ai.lzy.v1.portal.LzyPortalApi;
import ai.lzy.v1.portal.LzyPortalApi.PortalStatusRequest;
import ai.lzy.v1.portal.LzyPortalGrpc;
import ai.lzy.v1.slots.LSA;
import ai.lzy.v1.slots.LzySlotsApiGrpc;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.google.common.net.HostAndPort;
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
import java.nio.file.Files;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static ai.lzy.channelmanager.grpc.ProtoConverter.makeCreateDirectChannelCommand;
import static ai.lzy.channelmanager.grpc.ProtoConverter.makeDestroyChannelCommand;
import static ai.lzy.util.grpc.GrpcUtils.*;

public class PortalTestBase {
    private static final BaseTestWithIam iamTestContext = new BaseTestWithIam();

    @Rule
    public PreparedDbRule iamDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});

    private static final Logger LOG = LogManager.getLogger(PortalTestBase.class);

    private final ApplicationContext context = ApplicationContext.run("test");

    protected SchedulerPrivateApiMock schedulerServer;
    private ChannelManagerMock channelManager;
    private Map<String, Worker> workers;
    private Portal portal;

    private static final int S3_PORT = 8001;
    protected static final String S3_ADDRESS = "http://localhost:" + S3_PORT;
    protected static final String BUCKET_NAME = "lzy-bucket";

    private String allocatorAndSchedulerAddress;
    private String channelManagerAddress;

    private S3Mock s3;

    private ManagedChannel portalApiChannel;
    private ManagedChannel portalSlotsChannel;
    protected LzyPortalGrpc.LzyPortalBlockingStub unauthorizedPortalClient;
    private LzyPortalGrpc.LzyPortalBlockingStub authorizedPortalClient;
    private LzySlotsApiGrpc.LzySlotsApiBlockingStub portalSlotsClient;

    @Before
    public void before() throws IOException {
        System.err.println("---> " + ForkJoinPool.commonPool().getParallelism());
        var iamDbConfig = DatabaseTestUtils.preparePostgresConfig("iam", iamDb.getConnectionInfo());
        iamTestContext.setUp(iamDbConfig);
        startS3();
        var config = context.getBean(PortalConfig.class);
        allocatorAndSchedulerAddress = config.getAllocatorAddress();
        String[] hostAndPort = allocatorAndSchedulerAddress.split(":");
        schedulerServer = new SchedulerPrivateApiMock(Integer.parseInt(hostAndPort[1]));
        schedulerServer.start();
        workers = new HashMap<>();
        channelManagerAddress = config.getChannelManagerAddress();
        channelManager = new ChannelManagerMock(HostAndPort.fromString(channelManagerAddress));
        channelManager.start();
        startPortal(config);
    }

    @After
    public void after() throws InterruptedException {
        iamTestContext.after();
        stopS3();
        shutdownAndAwaitTerminationPortal();
        channelManager.stop();
        schedulerServer.stop();
        for (var servant : workers.values()) {
            servant.stop();
        }
        schedulerServer = null;
        channelManager = null;
        workers = null;
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

    private void startPortal(PortalConfig config) {
        createChannel("portal:stdout");
        createChannel("portal:stderr");

        try {
            var agent = new AllocatorAgent("portal_token", "portal_vm", allocatorAndSchedulerAddress,
                Duration.ofSeconds(5));

            portal = new Portal(config, agent, "portal_token");
            portal.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        var internalUserCredentials = iamTestContext.getClientConfig().createRenewableToken();

        portalApiChannel = newGrpcChannel("localhost", config.getPortalApiPort(), LzyPortalGrpc.SERVICE_NAME);
        unauthorizedPortalClient = newBlockingClient(
            LzyPortalGrpc.newBlockingStub(portalApiChannel),
            "Test",
            NO_AUTH_TOKEN);

        authorizedPortalClient = newBlockingClient(unauthorizedPortalClient, "TestClient",
            () -> internalUserCredentials.get().token());

        portalSlotsChannel = newGrpcChannel("localhost", config.getSlotsApiPort(), LzyFsGrpc.SERVICE_NAME);
        portalSlotsClient = newBlockingClient(
            LzySlotsApiGrpc.newBlockingStub(portalSlotsChannel),
            "Test",
            NO_AUTH_TOKEN); // TODO: Auth
    }

    private void shutdownAndAwaitTerminationPortal() {
        destroyChannel("portal:stdout");
        destroyChannel("portal:stderr");

        portal.shutdown();
        portalApiChannel.shutdown();
        portalSlotsChannel.shutdown();

        try {
            if (!portal.awaitTermination(60, TimeUnit.SECONDS)) {
                portal.shutdownNow();
                if (!portal.awaitTermination(60, TimeUnit.SECONDS)) {
                    LOG.error("Portal did not terminate");
                }
            }
        } catch (InterruptedException e) {
            portal.shutdownNow();
        } finally {
            portalApiChannel.shutdownNow();
            portalSlotsChannel.shutdownNow();
        }
    }

    protected String prepareTask(int taskNum, boolean newWorker, boolean isInput, String snapshotId) {
        String taskId = "task_" + taskNum;

        String worker = null;
        if (newWorker) {
            worker = "servant_" + taskNum;
            startWorker(worker);
        }

        String channelName = "channel_" + taskNum;
        String[] stdChannelNames = {taskId + ":stdout", taskId + ":stderr"};

        createChannel(channelName);
        createChannel(stdChannelNames[0]);
        createChannel(stdChannelNames[1]);

        String slotName = "/portal_slot_" + taskNum;
        LMS.Slot slot = isInput ? GrpcUtils.makeInputFileSlot(slotName) : GrpcUtils.makeOutputFileSlot(slotName);

        openPortalSlots(LzyPortalApi.OpenSlotsRequest.newBuilder()
            .addSlots(LzyPortal.PortalSlotDesc.newBuilder()
                .setSnapshot(GrpcUtils.makeAmazonSnapshot(snapshotId, BUCKET_NAME, S3_ADDRESS))
                .setSlot(slot)
                .setChannelId(channelName)
                .build())
            .addSlots(LzyPortal.PortalSlotDesc.newBuilder()
                .setSlot(GrpcUtils.makeInputFileSlot("/portal_%s:stdout".formatted(taskId)))
                .setChannelId(stdChannelNames[0])
                .setStdout(GrpcUtils.makeStdoutStorage(taskId))
                .build())
            .addSlots(LzyPortal.PortalSlotDesc.newBuilder()
                .setSlot(GrpcUtils.makeInputFileSlot("/portal_%s:stderr".formatted(taskId)))
                .setChannelId(stdChannelNames[1])
                .setStderr(GrpcUtils.makeStderrStorage(taskId))
                .build())
            .build());

        return worker;
    }

    protected String startTask(int taskNum, String fuze, LMS.Slot slot, String specifiedWorker) {
        String taskId = "task_" + taskNum;
        String actualWorker = Objects.isNull(specifiedWorker) ? "servant_" + taskNum : specifiedWorker;

        schedulerServer.startWorker(actualWorker,
            LMO.TaskDesc.newBuilder()
                .setOperation(LMO.Operation.newBuilder()
                    .setName("zygote_" + taskNum)
                    .setCommand(fuze)
                    .setStdout(LMO.Operation.StdSlotDesc.newBuilder()
                        .setName("/dev/stdout")
                        .setChannelId(taskId + ":stdout")
                        .build())
                    .setStderr(LMO.Operation.StdSlotDesc.newBuilder()
                        .setName("/dev/stderr")
                        .setChannelId(taskId + ":stderr")
                        .build())
                    .addSlots(slot)
                    .build())
                .addSlotAssignments(LMO.SlotToChannelAssignment.newBuilder()
                    .setSlotName(slot.getName())
                    .setChannelId("channel_" + taskNum)
                    .build())
                .addSlotAssignments(LMO.SlotToChannelAssignment.newBuilder()
                    .setSlotName("/dev/stdout")
                    .setChannelId(taskId + ":stdout")
                    .build())
                .addSlotAssignments(LMO.SlotToChannelAssignment.newBuilder()
                    .setSlotName("/dev/stderr")
                    .setChannelId(taskId + ":stderr")
                    .build())
                .build(), taskId, "execution-id");

        return taskId;
    }

    protected void startWorker(String workerId) {
        var allocatorDuration = Duration.ofSeconds(5);
        var schedulerDuration = Duration.ofSeconds(1);
        String privateKey;
        try {
            var workerKeys = RsaUtils.generateRsaKeys();
            var publicKey = Files.readString(workerKeys.publicKeyPath());
            privateKey = Files.readString(workerKeys.privateKeyPath());
        } catch (Exception e) {
            LOG.error("Cannot build credentials for portal", e);
            throw new RuntimeException(e);
        }
        var worker = new Worker("workflow", workerId, UUID.randomUUID().toString(), allocatorAndSchedulerAddress,
            allocatorAndSchedulerAddress, allocatorDuration, schedulerDuration,
            GrpcUtils.rollPort(), GrpcUtils.rollPort(), "/tmp/lzy_" + workerId + "/", channelManagerAddress,
            "localhost", privateKey, "token_" + workerId);
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

    protected void createChannel(String name) {
        channelManager.create(makeCreateDirectChannelCommand(UUID.randomUUID().toString(), name),
            GrpcUtils.SuccessStreamObserver.wrap(
                status -> System.out.println("Channel '" + name + "' created: " + JsonUtils.printSingleLine(status))));
    }

    protected void destroyChannel(String name) {
        channelManager.destroy(makeDestroyChannelCommand(name), GrpcUtils.SuccessStreamObserver.wrap(
            status -> System.out.println("Channel '" + name + "' removed: " + JsonUtils.printSingleLine(status))));
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
        var outputSlotRef = Objects.requireNonNull(channelManager.get(channelName)).outputSlot;
        var portalSlot = outputSlotRef.get();
        int n = 100;
        while (portalSlot == null && n-- > 0) {
            LockSupport.parkNanos(Duration.ofMillis(10).toNanos());
            portalSlot = outputSlotRef.get();
        }

        Assert.assertNotNull(portalSlot);

        var iter = openOutputSlot(ProtoConverter.fromProto(portalSlot.getSlotInstance()));

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
}
