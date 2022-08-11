package ai.lzy.servant.portal;

import ai.lzy.model.GrpcConverter;
import ai.lzy.model.JsonUtils;
import ai.lzy.servant.agents.LzyAgentConfig;
import ai.lzy.servant.agents.LzyServant;
import ai.lzy.v1.LzyFsApi;
import ai.lzy.v1.LzyPortalApi;
import ai.lzy.v1.LzyPortalApi.PortalSlotDesc;
import ai.lzy.v1.Operations;
import ai.lzy.v1.Tasks;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import io.findify.s3mock.S3Mock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.locks.LockSupport;

import static ai.lzy.test.GrpcUtils.*;

public class PortalTest {

    private ServerMock server;
    private ChannelManagerMock channelManager;
    private Map<String, LzyServant> servants;

    private static final int S3_PORT = 8001;
    private static final String S3_ADDRESS = "http://localhost:" + S3_PORT;
    private static final String BUCKET_NAME = "lzy-snapshot-test-bucket";

    private final S3Mock s3 = new S3Mock.Builder().withPort(S3_PORT).withInMemoryBackend().build();
    private final AmazonS3 s3Client = AmazonS3ClientBuilder
        .standard()
        .withPathStyleAccessEnabled(true)
        .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(S3_ADDRESS, "us-west-2"))
        .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
        .build();

    @Before
    public void before() throws IOException {
        System.err.println("---> " + ForkJoinPool.commonPool().getParallelism());
        server = new ServerMock();
        server.start1();
        channelManager = new ChannelManagerMock();
        channelManager.start();
        servants = new HashMap<>();
    }

    @After
    public void after() throws InterruptedException, IOException {
        channelManager.stop();
        server.stop();
        for (var servant : servants.values()) {
            servant.close();
        }
        server = null;
        channelManager = null;
        servants = null;
    }

    @Test
    public void activatePortal() throws Exception {
        startServant("servant_1");

        server.waitServantStart("servant_1");
        server.assertPortalNotActive("servant_1");

        createChannel("servant_1:stdout");
        createChannel("servant_1:stderr");

        server.startPortalOn("servant_1");
        server.assertServantNotActive("servant_1");

        destroyChannel("servant_1:stdout");
        destroyChannel("servant_1:stderr");
    }

    // run 2 sequential tasks:
    // * first task writes on portal
    // * second task reads from portal
    //
    // both tasks transfer their stdout/stderr to portal
    public void runGeneralSnapshotOnPortalScenario(PortalSlotDesc.Builder inputSnapshot,
                                                   PortalSlotDesc.Builder outputSnapshot) throws Exception {
        // portal
        startServant("portal");
        server.waitServantStart("portal");
        createChannel("portal:stdout");
        createChannel("portal:stderr");
        server.startPortalOn("portal");

        var portalStdout = readPortalSlot("portal:stdout");
        var portalStderr = readPortalSlot("portal:stderr");

        // servant
        startServant("servant");
        server.waitServantStart("servant");

        // just for logs
        Thread.sleep(Duration.ofSeconds(1).toMillis());
        System.out.println("\n----- PREPARE PORTAL FOR TASK 1 -----------------------------------------\n");

        // create channels for task_1
        createChannel("channel_1");
        createChannel("task_1:stdout");
        createChannel("task_1:stderr");

        // configure portal to snapshot `channel-1` data
        server.openPortalSlots(LzyPortalApi.OpenSlotsRequest.newBuilder()
            .addSlots(inputSnapshot
                .setSlot(makeInputFileSlot("/portal_slot_1"))
                .setChannelId("channel_1")
                .build())
            .addSlots(LzyPortalApi.PortalSlotDesc.newBuilder()
                .setSlot(makeInputFileSlot("/portal_task_1:stdout"))
                .setChannelId("task_1:stdout")
                .setStdout(makeStdoutStorage("task_1"))
                .build())
            .addSlots(LzyPortalApi.PortalSlotDesc.newBuilder()
                .setSlot(makeInputFileSlot("/portal_task_1:stderr"))
                .setChannelId("task_1:stderr")
                .setStderr(makeStderrStorage("task_1"))
                .build())
            .build());

        // just for logs
        Thread.sleep(Duration.ofSeconds(1).toMillis());
        System.out.println("\n----- RUN TASK 1 -----------------------------------------\n");

        var taskOutputSlot = makeOutputFileSlot("/slot_1");

        // run task and store result at portal
        server.start("servant",
            Tasks.TaskSpec.newBuilder()
                .setTid("task_1")
                .setZygote(Operations.Zygote.newBuilder()
                    .setName("zygote_1")
                    .addSlots(taskOutputSlot)
                    .setFuze("echo 'i-am-a-hacker' > /tmp/lzy_servant/slot_1 && echo 'hello'")
                    .build())
                .addAssignments(Tasks.SlotAssignment.newBuilder()
                    .setTaskId("task_1")
                    .setSlot(taskOutputSlot)
                    .setBinding("channel_1")
                    .build())
                .addAssignments(Tasks.SlotAssignment.newBuilder()
                    .setTaskId("task_1")
                    .setSlot(makeOutputPipeSlot("/dev/stdout"))
                    .setBinding("task_1:stdout")
                    .build())
                .addAssignments(Tasks.SlotAssignment.newBuilder()
                    .setTaskId("task_1")
                    .setSlot(makeOutputPipeSlot("/dev/stderr"))
                    .setBinding("task_1:stderr")
                    .build())
                .build(),
            SuccessStreamObserver.wrap(state -> System.out.println("Progress: " + JsonUtils.printSingleLine(state))));

        server.waitTaskCompleted("servant", "task_1");
        Assert.assertEquals("task_1; hello\n", portalStdout.take());
        Assert.assertEquals("task_1; ", portalStdout.take());
        Assert.assertEquals("task_1; ", portalStderr.take());
        server.waitPortalCompleted();

        // task_1 clean up
        System.out.println("-- cleanup task1 scenario --");
        destroyChannel("channel_1");
        destroyChannel("task_1:stdout");
        destroyChannel("task_1:stderr");

        Thread.sleep(Duration.ofSeconds(1).toMillis());
        System.out.println("\n----- PREPARE PORTAL FOR TASK 2 -----------------------------------------\n");

        ///// consumer task  /////

        // create channels for task_2
        createChannel("channel_2");
        createChannel("task_2:stdout");
        createChannel("task_2:stderr");

        // open portal output slot
        server.openPortalSlots(LzyPortalApi.OpenSlotsRequest.newBuilder()
            .addSlots(outputSnapshot
                .setSlot(makeOutputFileSlot("/slot_2"))
                .setChannelId("channel_2"))
            .addSlots(LzyPortalApi.PortalSlotDesc.newBuilder()
                .setSlot(makeInputPipeSlot("/portal_task_2:stdout"))
                .setChannelId("task_2:stdout")
                .setStdout(makeStdoutStorage("task_2"))
                .build())
            .addSlots(LzyPortalApi.PortalSlotDesc.newBuilder()
                .setSlot(makeInputPipeSlot("/portal_task_2:stderr"))
                .setChannelId("task_2:stderr")
                .setStderr(makeStderrStorage("task_2"))
                .build())
            .build());

        Thread.sleep(Duration.ofSeconds(1).toMillis());
        System.out.println("\n----- RUN TASK 2 -----------------------------------------\n");

        var tmpFile = File.createTempFile("lzy", "test-result");
        tmpFile.deleteOnExit();

        var taskInputSlot = makeInputFileSlot("/slot_2");

        // run task and load data from portal
        server.start("servant",
            Tasks.TaskSpec.newBuilder()
                .setTid("task_2")
                .setZygote(Operations.Zygote.newBuilder()
                    .setName("zygote_2")
                    .addSlots(taskInputSlot)
                    .setFuze("echo 'x' && /tmp/lzy_servant/sbin/cat /tmp/lzy_servant/slot_2 > "
                        + tmpFile.getAbsolutePath())
                    .build())
                .addAssignments(Tasks.SlotAssignment.newBuilder()
                    .setTaskId("task_2")
                    .setSlot(taskInputSlot)
                    .setBinding("channel_2")
                    .build())
                .addAssignments(Tasks.SlotAssignment.newBuilder()
                    .setTaskId("task_2")
                    .setSlot(makeOutputPipeSlot("/dev/stdout"))
                    .setBinding("task_2:stdout")
                    .build())
                .addAssignments(Tasks.SlotAssignment.newBuilder()
                    .setTaskId("task_2")
                    .setSlot(makeOutputPipeSlot("/dev/stderr"))
                    .setBinding("task_2:stderr")
                    .build())
                .build(),
            SuccessStreamObserver.wrap(state -> System.out.println("Progress: " + JsonUtils.printSingleLine(state))));

        // wait
        server.waitTaskCompleted("servant", "task_2");
        Assert.assertEquals("task_2; x\n", portalStdout.take());
        Assert.assertEquals("task_2; ", portalStdout.take());
        Assert.assertEquals("task_2; ", portalStderr.take());
        server.waitPortalCompleted();

        // task_2 clean up
        System.out.println("-- cleanup task2 scenario --");
        destroyChannel("channel_2");
        destroyChannel("task_2:stdout");
        destroyChannel("task_2:stderr");

        Thread.sleep(Duration.ofSeconds(1).toMillis());
        System.out.println("\n----------------------------------------------\n");

        Assert.assertTrue(portalStdout.isEmpty());
        Assert.assertTrue(portalStderr.isEmpty());
        destroyChannel("portal:stdout");
        destroyChannel("portal:stderr");

        var result = new String(Files.readAllBytes(tmpFile.toPath()));
        Assert.assertEquals("i-am-a-hacker\n", result);
    }

    @Test
    public void testSnapshotOnPortal() throws Exception {
        var inputSnapshot = LzyPortalApi.PortalSlotDesc.newBuilder()
            .setSnapshot(makeLocalSnapshot("snapshot_1"));
        var outputSnapshot = LzyPortalApi.PortalSlotDesc.newBuilder()
            .setSnapshot(makeLocalSnapshot("snapshot_1"));
        runGeneralSnapshotOnPortalScenario(inputSnapshot, outputSnapshot);
    }

    private void setUpS3() {
        s3.start();
        s3Client.createBucket(BUCKET_NAME);
    }

    private void tearDownS3() {
        s3.shutdown();
    }

    @Test
    public void testAmazonS3SnapshotOnPortal() throws Exception {
        setUpS3();

        var inputS3SnapshotSlot = LzyPortalApi.PortalSlotDesc.newBuilder()
            .setSnapshot(makeAmazonSnapshot("portal_slot_task_1", BUCKET_NAME, S3_ADDRESS));
        var outputS3SnapshotSlot = LzyPortalApi.PortalSlotDesc.newBuilder()
            .setSnapshot(makeAmazonSnapshot("portal_slot_task_1", BUCKET_NAME, S3_ADDRESS));
        runGeneralSnapshotOnPortalScenario(inputS3SnapshotSlot, outputS3SnapshotSlot);

        tearDownS3();
    }

    @Test
    public void testMultipleTasks() throws Exception {
        // portal
        startServant("portal");
        server.waitServantStart("portal");
        createChannel("portal:stdout");
        createChannel("portal:stderr");
        server.startPortalOn("portal");

        var portalStdout = readPortalSlot("portal:stdout");
        var portalStderr = readPortalSlot("portal:stderr");

        // servant_1
        startServant("servant_1");
        server.waitServantStart("servant_1");

        // servant_2
        startServant("servant_2");
        server.waitServantStart("servant_2");

        // just for logs
        Thread.sleep(Duration.ofSeconds(1).toMillis());
        System.out.println("\n----- PREPARE PORTAL FOR TASKS -----------------------------------------\n");

        // create channels for task_1
        createChannel("channel_1");
        createChannel("task_1:stdout");
        createChannel("task_1:stderr");

        // create channels for task_2
        createChannel("channel_2");
        createChannel("task_2:stdout");
        createChannel("task_2:stderr");

        // configure portal
        server.openPortalSlots(LzyPortalApi.OpenSlotsRequest.newBuilder()
            // task_1
            .addSlots(LzyPortalApi.PortalSlotDesc.newBuilder()
                .setSlot(makeInputFileSlot("/portal_slot_1"))
                .setChannelId("channel_1")
                .setSnapshot(makeLocalSnapshot("snapshot_1"))
                .build())
            .addSlots(LzyPortalApi.PortalSlotDesc.newBuilder()
                .setSlot(makeInputFileSlot("/portal_task_1:stdout"))
                .setChannelId("task_1:stdout")
                .setStdout(makeStdoutStorage("task_1"))
                .build())
            .addSlots(LzyPortalApi.PortalSlotDesc.newBuilder()
                .setSlot(makeInputFileSlot("/portal_task_1:stderr"))
                .setChannelId("task_1:stderr")
                .setStderr(makeStderrStorage("task_1"))
                .build())
            // task_2
            .addSlots(LzyPortalApi.PortalSlotDesc.newBuilder()
                .setSlot(makeInputFileSlot("/portal_slot_2"))
                .setChannelId("channel_2")
                .setSnapshot(makeLocalSnapshot("snapshot_2"))
                .build())
            .addSlots(LzyPortalApi.PortalSlotDesc.newBuilder()
                .setSlot(makeInputFileSlot("/portal_task_2:stdout"))
                .setChannelId("task_2:stdout")
                .setStdout(makeStdoutStorage("task_2"))
                .build())
            .addSlots(LzyPortalApi.PortalSlotDesc.newBuilder()
                .setSlot(makeInputFileSlot("/portal_task_2:stderr"))
                .setChannelId("task_2:stderr")
                .setStderr(makeStderrStorage("task_2"))
                .build())
            .build());

        // just for logs
        Thread.sleep(Duration.ofSeconds(1).toMillis());
        System.out.println("\n----- RUN TASKS -----------------------------------------\n");

        var task1OutputSlot = makeOutputFileSlot("/slot_1");
        var task2OutputSlot = makeOutputFileSlot("/slot_2");

        // run task_1 on servant_1
        server.start("servant_1",
            Tasks.TaskSpec.newBuilder()
                .setTid("task_1")
                .setZygote(Operations.Zygote.newBuilder()
                    .setName("zygote_1")
                    .addSlots(task1OutputSlot)
                    .setFuze("echo 'hello from task_1' > /tmp/lzy_servant_1/slot_1 && echo 'hello from task_1'")
                    .build())
                .addAssignments(Tasks.SlotAssignment.newBuilder()
                    .setTaskId("task_1")
                    .setSlot(task1OutputSlot)
                    .setBinding("channel_1")
                    .build())
                .addAssignments(Tasks.SlotAssignment.newBuilder()
                    .setTaskId("task_1")
                    .setSlot(makeOutputPipeSlot("/dev/stdout"))
                    .setBinding("task_1:stdout")
                    .build())
                .addAssignments(Tasks.SlotAssignment.newBuilder()
                    .setTaskId("task_1")
                    .setSlot(makeOutputPipeSlot("/dev/stderr"))
                    .setBinding("task_1:stderr")
                    .build())
                .build(),
            SuccessStreamObserver.wrap(state -> System.out.println("Progress: " + JsonUtils.printSingleLine(state))));

        // run task_2 on servant_2
        server.start("servant_2",
            Tasks.TaskSpec.newBuilder()
                .setTid("task_2")
                .setZygote(Operations.Zygote.newBuilder()
                    .setName("zygote_2")
                    .addSlots(task2OutputSlot)
                    .setFuze("echo 'hello from task_2' > /tmp/lzy_servant_2/slot_2 && echo 'hello from task_2'")
                    .build())
                .addAssignments(Tasks.SlotAssignment.newBuilder()
                    .setTaskId("task_2")
                    .setSlot(task2OutputSlot)
                    .setBinding("channel_2")
                    .build())
                .addAssignments(Tasks.SlotAssignment.newBuilder()
                    .setTaskId("task_2")
                    .setSlot(makeOutputPipeSlot("/dev/stdout"))
                    .setBinding("task_2:stdout")
                    .build())
                .addAssignments(Tasks.SlotAssignment.newBuilder()
                    .setTaskId("task_2")
                    .setSlot(makeOutputPipeSlot("/dev/stderr"))
                    .setBinding("task_2:stderr")
                    .build())
                .build(),
            SuccessStreamObserver.wrap(state -> System.out.println("Progress: " + JsonUtils.printSingleLine(state))));

        server.waitTaskCompleted("servant_1", "task_1");
        server.waitTaskCompleted("servant_2", "task_2");
        server.waitPortalCompleted();

        Thread.sleep(Duration.ofSeconds(1).toMillis());
        System.out.println("\n----- TASKS DONE -----------------------------------------\n");

        System.out.println("-- cleanup tasks --");
        destroyChannel("channel_1");
        destroyChannel("task_1:stdout");
        destroyChannel("task_1:stderr");
        destroyChannel("channel_2");
        destroyChannel("task_2:stdout");
        destroyChannel("task_2:stderr");

        var expected = new HashSet<String>() {
            {
                add("task_1; hello from task_1\n");
                add("task_1; ");
                add("task_2; hello from task_2\n");
                add("task_2; ");
            }
        };

        while (!expected.isEmpty()) {
            var actual = portalStdout.take();
            Assert.assertTrue(actual.toString(), actual instanceof String);
            Assert.assertTrue(actual.toString(), expected.remove(actual));
        }
        Assert.assertNull(portalStdout.poll());

        expected.add("task_1; ");
        expected.add("task_2; ");

        while (!expected.isEmpty()) {
            var actual = portalStderr.take();
            Assert.assertTrue(actual.toString(), actual instanceof String);
            Assert.assertTrue(actual.toString(), expected.remove(actual));
        }
        Assert.assertNull(portalStderr.poll());

        destroyChannel("portal:stdout");
        destroyChannel("portal:stderr");
    }

    private void startServant(String servantId) throws URISyntaxException, IOException {
        var servant = new LzyServant(LzyAgentConfig.builder()
            .serverAddress(URI.create("grpc://localhost:" + server.port()))
            .whiteboardAddress(URI.create("grpc://localhost:" + rollPort()))
            .channelManagerAddress(URI.create("grpc://localhost:" + channelManager.port()))
            .servantId(servantId)
            .token("token_" + servantId)
            .bucket("bucket_" + servantId)
            .scheme("servant")
            .agentHost("localhost")
            .agentPort(rollPort())
            .fsPort(rollPort())
            .root(Path.of("/tmp/lzy_" + servantId + "/"))
            .build());
        servants.put(servantId, servant);
    }

    private void createChannel(String name) {
        channelManager.create(makeCreateDirectChannelCommand(UUID.randomUUID().toString(), name),
            SuccessStreamObserver.wrap(
                status -> System.out.println("Channel '" + name + "' created: " + JsonUtils.printSingleLine(status))));
    }

    private void destroyChannel(String name) {
        channelManager.destroy(makeDestroyChannelCommand(name), SuccessStreamObserver.wrap(
            status -> System.out.println("Channel '" + name + "' removed: " + JsonUtils.printSingleLine(status))));
    }

    private ArrayBlockingQueue<Object> readPortalSlot(String channelName) {
        var outputSlotRef = Objects.requireNonNull(channelManager.get(channelName)).outputSlot;
        var portalSlot = outputSlotRef.get();
        int n = 100;
        while (portalSlot == null && n-- > 0) {
            LockSupport.parkNanos(Duration.ofMillis(10).toNanos());
            portalSlot = outputSlotRef.get();
        }

        Assert.assertNotNull(portalSlot);

        var iter = server.portal().openOutputSlot(GrpcConverter.from(portalSlot.getSlotInstance()));

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
