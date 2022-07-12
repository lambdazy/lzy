package ai.lzy.servant.portal;

import ai.lzy.model.JsonUtils;
import ai.lzy.priv.v2.*;
import ai.lzy.servant.agents.LzyAgentConfig;
import ai.lzy.servant.agents.LzyServant;
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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import static ai.lzy.servant.portal.Utils.*;

public class PortalTest {
    private ServerMock server;
    private Map<String, LzyServant> servants;

    @Before
    public void before() throws IOException {
        System.err.println("---> " + ForkJoinPool.commonPool().getParallelism());
        server = new ServerMock();
        server.start1();
        servants = new HashMap<>();
    }

    @After
    public void after() throws InterruptedException {
        server.stop();
        for (var servant : servants.values()) {
            servant.close();
        }
        server = null;
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
    @Test
    public void testSnapshotOnPortal() throws Exception {
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
        createChannel("task_1:stdin");
        createChannel("task_1:stdout");
        createChannel("task_1:stderr");

        // configure portal to snapshot `channel-1` data
        server.openPortalSlots(LzyPortalApi.OpenSlotsRequest.newBuilder()
            .addSlots(LzyPortalApi.PortalSlotDesc.newBuilder()
                .setSlot(makeInputFileSlot("/portal_slot_1"))
                .setChannelId("channel_1")
                .setSnapshot(makeSnapshotStorage("snapshot_1"))
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

        var taskOutputSlot = Operations.Slot.newBuilder()
            .setName("/slot_1")
            .setMedia(Operations.Slot.Media.FILE)
            .setDirection(Operations.Slot.Direction.OUTPUT)
            .setContentType(makePlainTextDataScheme())
            .build();

        // run task and store result at portal
        server.start(Tasks.TaskSpec.newBuilder()
            .setTid("task_1")
            .setZygote(Operations.Zygote.newBuilder()
                .setName("zygote_1")
                .addSlots(taskOutputSlot)
                .setFuze("echo 'i-am-a-hacker' > /tmp/lzy_servant/slot_1 && echo 'hello'")
                .build())
            .addAssignments(Tasks.SlotAssignment.newBuilder()
                .setTaskId("task_1")
                .setSlot(taskOutputSlot)
                .setBinding("channel:channel_1")
                .build())
            .addAssignments(Tasks.SlotAssignment.newBuilder()
                .setTaskId("task_1")
                .setSlot(makeOutputPipeSlot("/dev/stdout"))
                .setBinding("channel:task_1:stdout")
                .build())
            .addAssignments(Tasks.SlotAssignment.newBuilder()
                .setTaskId("task_1")
                .setSlot(makeOutputPipeSlot("/dev/stderr"))
                .setBinding("channel:task_1:stderr")
                .build())
            .build(),
            SuccessStreamObserver.wrap(state -> System.out.println("Progress: " + JsonUtils.printSingleLine(state))));

        server.waitTaskCompleted("task_1");
        Assert.assertEquals("task_1; hello\n", portalStdout.take());
        Assert.assertEquals("task_1; ", portalStdout.take());
        Assert.assertEquals("task_1; ", portalStderr.take());
        server.waitPortalCompleted();

        // task_1 clean up
        System.out.println("-- cleanup task1 scenario --");
        destroyChannel("channel_1");
        destroyChannel("task_1:stdin");
        destroyChannel("task_1:stdout");
        destroyChannel("task_1:stderr");

        Thread.sleep(Duration.ofSeconds(1).toMillis());
        System.out.println("\n----- PREPARE PORTAL FOR TASK 2 -----------------------------------------\n");


        ///// consumer task  /////

        // create channels for task_2
        createChannel("channel_2");
        createChannel("task_2:stdin");
        createChannel("task_2:stdout");
        createChannel("task_2:stderr");

        // open portal output slot
        server.openPortalSlots(LzyPortalApi.OpenSlotsRequest.newBuilder()
            .addSlots(LzyPortalApi.PortalSlotDesc.newBuilder()
                .setSlot(makeOutputFileSlot("/slot_2"))
                .setChannelId("channel_2")
                .setSnapshot(makeSnapshotStorage("snapshot_1"))
                .build())
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
        server.start(Tasks.TaskSpec.newBuilder()
            .setTid("task_2")
            .setZygote(Operations.Zygote.newBuilder()
                .setName("zygote_2")
                .addSlots(taskInputSlot)
                .setFuze("echo 'x' && /tmp/lzy_servant/sbin/cat /tmp/lzy_servant/slot_2 > " + tmpFile.getAbsolutePath())
                .build())
            .addAssignments(Tasks.SlotAssignment.newBuilder()
                .setTaskId("task_2")
                .setSlot(taskInputSlot)
                .setBinding("channel:channel_2")
                .build())
            .addAssignments(Tasks.SlotAssignment.newBuilder()
                .setTaskId("task_2")
                .setSlot(makeOutputPipeSlot("/dev/stdout"))
                .setBinding("channel:task_2:stdout")
                .build())
            .addAssignments(Tasks.SlotAssignment.newBuilder()
                .setTaskId("task_2")
                .setSlot(makeOutputPipeSlot("/dev/stderr"))
                .setBinding("channel:task_2:stderr")
                .build())
            .build(),
            SuccessStreamObserver.wrap(state -> System.out.println("Progress: " + JsonUtils.printSingleLine(state))));

        // wait
        server.waitTaskCompleted("task_2");
        Assert.assertEquals("task_2; x\n", portalStdout.take());
        Assert.assertEquals("task_2; ", portalStdout.take());
        Assert.assertEquals("task_2; ", portalStderr.take());
        server.waitPortalCompleted();

        // task_2 clean up
        System.out.println("-- cleanup task2 scenario --");
        destroyChannel("channel_2");
        destroyChannel("task_2:stdin");
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

    private void startServant(String servantId) throws URISyntaxException, IOException {
        var servant = new LzyServant(LzyAgentConfig.builder()
                .serverAddress(URI.create("grpc://localhost:" + server.port()))
                .whiteboardAddress(URI.create("grpc://localhost:" + rollPort()))
                .servantId(servantId)
                .token("token_" + servantId)
                .bucket("bucket_" + servantId)
                .agentHost("localhost")
                .agentPort(rollPort())
                .fsPort(rollPort())
                .root(Path.of("/tmp/lzy_" + servantId + "/"))
                .build());
        servant.start();
        servants.put(servantId, servant);
    }

    private void createChannel(String name) {
        server.channel(makeCreateDirectChannelCommand(name), SuccessStreamObserver.wrap(
            status -> System.out.println("Channel '" + name + "' created: " + JsonUtils.printSingleLine(status))));
    }

    private void destroyChannel(String name) {
        server.channel(makeDestroyChannelCommand(name), SuccessStreamObserver.wrap(
            status -> System.out.println("Channel '" + name + "' removed: " + JsonUtils.printSingleLine(status))));
    }

    private ArrayBlockingQueue<Object> readPortalSlot(String channel) {
        var outputSlotRef = server.directChannels.get(channel).outputSlot;
        var portalSlot = outputSlotRef.get();
        int n = 100;
        while (portalSlot == null && n-- > 0) {
            LockSupport.parkNanos(Duration.ofMillis(10).toNanos());
            portalSlot = outputSlotRef.get();
        }

        Assert.assertNotNull(portalSlot);

        var iter = server.portal().openOutputSlot(portalSlot.getUri());

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
