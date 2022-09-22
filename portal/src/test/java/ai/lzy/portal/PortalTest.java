package ai.lzy.portal;

import ai.lzy.allocator.AllocatorAgent;
import ai.lzy.channelmanager.grpc.ChannelManagerMock;
import ai.lzy.fs.LzyFsServer;
import ai.lzy.model.grpc.ProtoConverter;
import ai.lzy.model.slot.SlotInstance;
import ai.lzy.portal.config.PortalConfig;
import ai.lzy.servant.agents.LzyAgentConfig;
import ai.lzy.servant.agents.LzyServant;
import ai.lzy.test.GrpcUtils;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.util.grpc.JsonUtils;
import ai.lzy.v1.common.LMS;
import ai.lzy.v1.deprecated.LzyTask;
import ai.lzy.v1.deprecated.LzyZygote;
import ai.lzy.v1.fs.LzyFsApi;
import ai.lzy.v1.fs.LzyFsGrpc;
import ai.lzy.v1.portal.LzyPortal;
import ai.lzy.v1.portal.LzyPortalApi;
import ai.lzy.v1.portal.LzyPortalGrpc;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.google.common.net.HostAndPort;
import com.google.protobuf.Empty;
import io.findify.s3mock.S3Mock;
import io.grpc.ManagedChannel;
import io.micronaut.context.ApplicationContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static ai.lzy.channelmanager.grpc.ProtoConverter.makeCreateDirectChannelCommand;
import static ai.lzy.channelmanager.grpc.ProtoConverter.makeDestroyChannelCommand;
import static ai.lzy.model.UriScheme.LzyFs;

public abstract class PortalTest {
    private static final Logger LOG = LogManager.getLogger(PortalTest.class);

    private final ApplicationContext context = ApplicationContext.run("test");

    protected ServerMock server;
    private ChannelManagerMock channelManager;
    private Map<String, LzyServant> servants;
    private Portal portal;

    private static final int S3_PORT = 8001;
    protected static final String S3_ADDRESS = "http://localhost:" + S3_PORT;
    protected static final String BUCKET_NAME = "lzy-bucket";

    private S3Mock s3;

    private LzyPortalGrpc.LzyPortalBlockingStub portalStub;
    private LzyFsGrpc.LzyFsBlockingStub portalFsStub;

    @Before
    public void before() throws IOException {
        System.err.println("---> " + ForkJoinPool.commonPool().getParallelism());
        startS3();
        server = new ServerMock();
        server.startup();
        servants = new HashMap<>();
        var config = context.getBean(PortalConfig.class);
        //noinspection UnstableApiUsage
        channelManager = new ChannelManagerMock(HostAndPort.fromString(config.getChannelManagerAddress()));
        channelManager.start();
        startPortal(config);
    }

    @After
    public void after() throws InterruptedException, IOException {
        stopS3();
        shutdownAndAwaitTerminationPortal();
        channelManager.stop();
        server.stop();
        for (var servant : servants.values()) {
            servant.close();
        }
        server = null;
        channelManager = null;
        servants = null;
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
            var fsUri = new URI(LzyFs.scheme(), null, config.getHost(), config.getFsApiPort(), null, null, null);
            var cm = HostAndPort.fromString(config.getChannelManagerAddress());
            var channelManagerUri = new URI("http", null, cm.getHost(), cm.getPort(), null, null, null);

            var agent = new AllocatorAgent("portal_token", "portal_vm",
                "localhost:" + server.port, Duration.ofSeconds(5), "localhost");
            var fs = new LzyFsServer(config.getPortalId(), config.getFsRoot(), fsUri, channelManagerUri,
                "portal_token");

            portal = new Portal(config, agent, fs);
            portal.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        portalStub = LzyPortalGrpc.newBlockingStub(
            ChannelBuilder.forAddress("localhost", config.getPortalApiPort())
                .usePlaintext()
                .enableRetry(LzyPortalGrpc.SERVICE_NAME)
                .build());
        portalFsStub = LzyFsGrpc.newBlockingStub(
            ChannelBuilder.forAddress("localhost", config.getFsApiPort())
                .usePlaintext()
                .enableRetry(LzyFsGrpc.SERVICE_NAME)
                .build());
    }

    private void shutdownAndAwaitTerminationPortal() {
        destroyChannel("portal:stdout");
        destroyChannel("portal:stderr");

        var portalStubChannel = (ManagedChannel) portalStub.getChannel();
        var portalFsStubChannel = (ManagedChannel) portalFsStub.getChannel();

        portal.shutdown();
        portalStubChannel.shutdown();
        portalFsStubChannel.shutdown();

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
            portalStubChannel.shutdownNow();
            portalFsStubChannel.shutdownNow();
        }
    }

    protected String prepareTask(int taskId, boolean newServant, boolean isInput, String snapshotId) throws Exception {
        String taskName = "task_" + taskId;

        String servant = null;
        if (newServant) {
            servant = "servant_" + taskId;
            startServant(servant);
            server.waitServantStart(servant);
        }

        String channelName = "channel_" + taskId;
        String[] stdChannelNames = {taskName + ":stdout", taskName + ":stderr"};

        createChannel(channelName);
        createChannel(stdChannelNames[0]);
        createChannel(stdChannelNames[1]);

        String slotName = "/portal_slot_" + taskId;
        LMS.Slot slot = isInput ? GrpcUtils.makeInputFileSlot(slotName) : GrpcUtils.makeOutputFileSlot(slotName);

        openPortalSlots(LzyPortalApi.OpenSlotsRequest.newBuilder()
            .addSlots(LzyPortal.PortalSlotDesc.newBuilder()
                .setSnapshot(GrpcUtils.makeAmazonSnapshot(snapshotId, BUCKET_NAME, S3_ADDRESS))
                .setSlot(slot)
                .setChannelId(channelName)
                .build())
            .addSlots(LzyPortal.PortalSlotDesc.newBuilder()
                .setSlot(GrpcUtils.makeInputFileSlot("/portal_%s:stdout".formatted(taskName)))
                .setChannelId(stdChannelNames[0])
                .setStdout(GrpcUtils.makeStdoutStorage(taskName))
                .build())
            .addSlots(LzyPortal.PortalSlotDesc.newBuilder()
                .setSlot(GrpcUtils.makeInputFileSlot("/portal_%s:stderr".formatted(taskName)))
                .setChannelId(stdChannelNames[1])
                .setStderr(GrpcUtils.makeStderrStorage(taskName))
                .build())
            .build());

        return servant;
    }

    protected String startTask(int taskNum, String fuze, LMS.Slot slot, String specifiedServant) {
        String taskId = "task_" + taskNum;
        String actualServant = Objects.isNull(specifiedServant) ? "servant_" + taskNum : specifiedServant;

        server.start(actualServant,
            LzyTask.TaskSpec.newBuilder()
                .setTid(taskId)
                .setZygote(LzyZygote.Zygote.newBuilder()
                    .setName("zygote_" + taskNum)
                    .addSlots(slot)
                    .setFuze(fuze)
                    .build())
                .addAssignments(LzyTask.SlotAssignment.newBuilder()
                    .setTaskId(taskId)
                    .setSlot(slot)
                    .setBinding("channel_" + taskNum)
                    .build())
                .addAssignments(LzyTask.SlotAssignment.newBuilder()
                    .setTaskId(taskId)
                    .setSlot(GrpcUtils.makeOutputPipeSlot("/dev/stdout"))
                    .setBinding(taskId + ":stdout")
                    .build())
                .addAssignments(LzyTask.SlotAssignment.newBuilder()
                    .setTaskId(taskId)
                    .setSlot(GrpcUtils.makeOutputPipeSlot("/dev/stderr"))
                    .setBinding(taskId + ":stderr")
                    .build())
                .build(),
            GrpcUtils.SuccessStreamObserver.wrap(
                state -> System.out.println("Progress: " + JsonUtils.printSingleLine(state))));

        return taskId;
    }

    protected void startServant(String servantId) throws URISyntaxException, IOException {
        var servant = new LzyServant(LzyAgentConfig.builder()
            .serverAddress(URI.create("grpc://localhost:" + server.port()))
            .whiteboardAddress(URI.create("grpc://localhost:" + GrpcUtils.rollPort()))
            .channelManagerAddress(URI.create("grpc://localhost:" + channelManager.port()))
            .servantId(servantId)
            .token("token_" + servantId)
            .bucket("bucket_" + servantId)
            .scheme("servant")
            .agentHost("localhost")
            .agentPort(GrpcUtils.rollPort())
            .fsPort(GrpcUtils.rollPort())
            .root(Path.of("/tmp/lzy_" + servantId + "/"))
            .build());
        servants.put(servantId, servant);
    }

    protected void waitPortalCompleted() {
        boolean done = false;
        while (!done) {
            var status = portalStub.status(Empty.getDefaultInstance());
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
        var response = portalStub.openSlots(request);
        Assert.assertTrue(response.getDescription(), response.getSuccess());
    }

    protected String openPortalSlotsWithFail(LzyPortalApi.OpenSlotsRequest request) {
        var response = portalStub.openSlots(request);
        Assert.assertFalse(response.getSuccess());
        return response.getDescription();
    }

    protected Iterator<LzyFsApi.Message> openOutputSlot(SlotInstance slot) {
        return portalFsStub.openOutputSlot(
            LzyFsApi.SlotRequest.newBuilder()
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
                    switch (message.getMessageCase()) {
                        case CONTROL -> {
                            if (LzyFsApi.Message.Controls.EOS != message.getControl()) {
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
