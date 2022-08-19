package ai.lzy.servant.portal;

import ai.lzy.model.JsonUtils;
import ai.lzy.v1.LzyPortalApi;
import ai.lzy.v1.Operations;
import ai.lzy.v1.Tasks;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.time.Duration;
import java.util.HashSet;

import static ai.lzy.test.GrpcUtils.*;
import static ai.lzy.test.GrpcUtils.makeOutputPipeSlot;

public class UsualCasesPortalTest extends PortalTest {
    @Test
    public void makeSnapshotOnPortalThenReadItTest() throws Exception {
        runWithS3(this::firstTaskWriteSnapshotSecondReadIt);
    }

    @Test
    public void multipleTasksMakeSnapshotsTest() throws Exception {
        runWithS3(this::runMultipleTasks);
    }

    @Test
    public void multipleSequentialConsumerAndSingleSnapshotProducerTest() throws Exception {
        runWithS3(this::singleSnapshotMultipleConsumers);
    }

    @Test
    public void multipleConcurrentConsumerAndSingleSnapshotProducerTest() throws Exception {
        runWithS3(this::singleSnapshotMultipleConsumersConcurrent);
    }
    
    // run 2 sequential tasks:
    // * first task writes on portal
    // * second task reads from portal
    //
    // both tasks transfer their stdout/stderr to portal
    private void firstTaskWriteSnapshotSecondReadIt() throws Exception {
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
            .addSlots(LzyPortalApi.PortalSlotDesc.newBuilder()
                .setSnapshot(makeAmazonSnapshot("portal_slot_task_1", BUCKET_NAME, S3_ADDRESS))
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
            .addSlots(LzyPortalApi.PortalSlotDesc.newBuilder()
                .setSnapshot(makeAmazonSnapshot("portal_slot_task_1", BUCKET_NAME, S3_ADDRESS))
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

    public void runMultipleTasks() throws Exception {
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
                .setSnapshot(makeAmazonSnapshot("snapshot_1", BUCKET_NAME, S3_ADDRESS))
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
                .setSnapshot(makeAmazonSnapshot("snapshot_2", BUCKET_NAME, S3_ADDRESS))
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

    private void singleSnapshotMultipleConsumers() throws Exception {
        // portal
        startServant("portal");
        server.waitServantStart("portal");
        createChannel("portal:stdout");
        createChannel("portal:stderr");
        server.startPortalOn("portal");

        var portalStdout = readPortalSlot("portal:stdout");
        var portalStderr = readPortalSlot("portal:stderr");

        // servants
        startServant("servant_1");
        server.waitServantStart("servant_1");

        startServant("servant_2");
        server.waitServantStart("servant_2");

        startServant("servant_3");
        server.waitServantStart("servant_3");


        // just for logs
        Thread.sleep(Duration.ofSeconds(1).toMillis());
        System.out.println("\n----- PREPARE PORTAL FOR TASK 1 -----------------------------------------\n");

        // create channels for task_1
        createChannel("channel_1");
        createChannel("task_1:stdout");
        createChannel("task_1:stderr");

        // configure portal to snapshot `channel-1` data
        server.openPortalSlots(LzyPortalApi.OpenSlotsRequest.newBuilder()
            .addSlots(LzyPortalApi.PortalSlotDesc.newBuilder()
                .setSnapshot(makeAmazonSnapshot("snapshot_1", BUCKET_NAME, S3_ADDRESS))
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
        server.start("servant_1",
            Tasks.TaskSpec.newBuilder()
                .setTid("task_1")
                .setZygote(Operations.Zygote.newBuilder()
                    .setName("zygote_1")
                    .addSlots(taskOutputSlot)
                    .setFuze("echo 'i-am-a-hacker' > /tmp/lzy_servant_1/slot_1 && echo 'hello'")
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

        server.waitTaskCompleted("servant_1", "task_1");
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
        System.out.println("\n----- PREPARE PORTAL FOR TASK 2, TASK 3 -----------------------------------------\n");

        ///// consumer tasks  /////

        // create channels for task_2, task_3
        createChannel("channel_2");
        createChannel("task_2:stdout");
        createChannel("task_2:stderr");
        createChannel("channel_3");
        createChannel("task_3:stdout");
        createChannel("task_3:stderr");

        // open portal output slot
        server.openPortalSlots(LzyPortalApi.OpenSlotsRequest.newBuilder()
            .addSlots(LzyPortalApi.PortalSlotDesc.newBuilder()
                .setSnapshot(makeAmazonSnapshot("snapshot_1", BUCKET_NAME, S3_ADDRESS))
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

        // open portal output slot
        server.openPortalSlots(LzyPortalApi.OpenSlotsRequest.newBuilder()
            .addSlots(LzyPortalApi.PortalSlotDesc.newBuilder()
                .setSnapshot(makeAmazonSnapshot("snapshot_1", BUCKET_NAME, S3_ADDRESS))
                .setSlot(makeOutputFileSlot("/slot_3"))
                .setChannelId("channel_3"))
            .addSlots(LzyPortalApi.PortalSlotDesc.newBuilder()
                .setSlot(makeInputPipeSlot("/portal_task_3:stdout"))
                .setChannelId("task_3:stdout")
                .setStdout(makeStdoutStorage("task_3"))
                .build())
            .addSlots(LzyPortalApi.PortalSlotDesc.newBuilder()
                .setSlot(makeInputPipeSlot("/portal_task_3:stderr"))
                .setChannelId("task_3:stderr")
                .setStderr(makeStderrStorage("task_3"))
                .build())
            .build());


        Thread.sleep(Duration.ofSeconds(1).toMillis());
        System.out.println("\n----- RUN TASK 2 -----------------------------------------\n");

        var tmpFile2 = File.createTempFile("lzy", "test-result-2");
        tmpFile2.deleteOnExit();

        var taskInputSlot2 = makeInputFileSlot("/slot_2");

        // run task and load data from portal
        server.start("servant_2",
            Tasks.TaskSpec.newBuilder()
                .setTid("task_2")
                .setZygote(Operations.Zygote.newBuilder()
                    .setName("zygote_2")
                    .addSlots(taskInputSlot2)
                    .setFuze("echo 'x' && /tmp/lzy_servant_2/sbin/cat /tmp/lzy_servant_2/slot_2 > "
                        + tmpFile2.getAbsolutePath())
                    .build())
                .addAssignments(Tasks.SlotAssignment.newBuilder()
                    .setTaskId("task_2")
                    .setSlot(taskInputSlot2)
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
        server.waitTaskCompleted("servant_2", "task_2");
        Assert.assertEquals("task_2; x\n", portalStdout.take());
        Assert.assertEquals("task_2; ", portalStdout.take());
        Assert.assertEquals("task_2; ", portalStderr.take());
        server.waitPortalCompleted();

        Thread.sleep(Duration.ofSeconds(1).toMillis());
        System.out.println("\n----- RUN TASK 3 -----------------------------------------\n");

        var tmpFile3 = File.createTempFile("lzy", "test-result-3");
        tmpFile3.deleteOnExit();

        var taskInputSlot3 = makeInputFileSlot("/slot_3");

        // run task and load data from portal
        server.start("servant_3",
            Tasks.TaskSpec.newBuilder()
                .setTid("task_3")
                .setZygote(Operations.Zygote.newBuilder()
                    .setName("zygote_3")
                    .addSlots(taskInputSlot3)
                    .setFuze("echo 'x' && /tmp/lzy_servant_3/sbin/cat /tmp/lzy_servant_3/slot_3 > "
                        + tmpFile3.getAbsolutePath())
                    .build())
                .addAssignments(Tasks.SlotAssignment.newBuilder()
                    .setTaskId("task_3")
                    .setSlot(taskInputSlot3)
                    .setBinding("channel_3")
                    .build())
                .addAssignments(Tasks.SlotAssignment.newBuilder()
                    .setTaskId("task_3")
                    .setSlot(makeOutputPipeSlot("/dev/stdout"))
                    .setBinding("task_3:stdout")
                    .build())
                .addAssignments(Tasks.SlotAssignment.newBuilder()
                    .setTaskId("task_3")
                    .setSlot(makeOutputPipeSlot("/dev/stderr"))
                    .setBinding("task_3:stderr")
                    .build())
                .build(),
            SuccessStreamObserver.wrap(state -> System.out.println("Progress: " + JsonUtils.printSingleLine(state))));

        // wait
        server.waitTaskCompleted("servant_3", "task_3");
        Assert.assertEquals("task_3; x\n", portalStdout.take());
        Assert.assertEquals("task_3; ", portalStdout.take());
        Assert.assertEquals("task_3; ", portalStderr.take());
        server.waitPortalCompleted();

        // task_3 clean up
        // task_2 clean up
        System.out.println("-- cleanup task_2 scenario --");
        destroyChannel("channel_2");
        destroyChannel("task_2:stdout");
        destroyChannel("task_2:stderr");

        System.out.println("-- cleanup task_3 scenario --");
        destroyChannel("channel_3");
        destroyChannel("task_3:stdout");
        destroyChannel("task_3:stderr");


        Thread.sleep(Duration.ofSeconds(1).toMillis());
        System.out.println("\n----------------------------------------------\n");

        Assert.assertTrue(portalStdout.isEmpty());
        Assert.assertTrue(portalStderr.isEmpty());
        destroyChannel("portal:stdout");
        destroyChannel("portal:stderr");

        var result2 = new String(Files.readAllBytes(tmpFile2.toPath()));
        var result3 = new String(Files.readAllBytes(tmpFile3.toPath()));
        Assert.assertEquals("i-am-a-hacker\n", result2);
        Assert.assertEquals("i-am-a-hacker\n", result3);
    }

    private void singleSnapshotMultipleConsumersConcurrent() throws Exception {
        // portal
        startServant("portal");
        server.waitServantStart("portal");
        createChannel("portal:stdout");
        createChannel("portal:stderr");
        server.startPortalOn("portal");

        var portalStdout = readPortalSlot("portal:stdout");
        var portalStderr = readPortalSlot("portal:stderr");

        // servants
        startServant("servant_1");
        server.waitServantStart("servant_1");

        startServant("servant_2");
        server.waitServantStart("servant_2");

        startServant("servant_3");
        server.waitServantStart("servant_3");


        // just for logs
        Thread.sleep(Duration.ofSeconds(1).toMillis());
        System.out.println("\n----- PREPARE PORTAL FOR TASK 1 -----------------------------------------\n");

        // create channels for task_1
        createChannel("channel_1");
        createChannel("task_1:stdout");
        createChannel("task_1:stderr");

        // configure portal to snapshot `channel-1` data
        server.openPortalSlots(LzyPortalApi.OpenSlotsRequest.newBuilder()
            .addSlots(LzyPortalApi.PortalSlotDesc.newBuilder()
                .setSnapshot(makeAmazonSnapshot("snapshot_1", BUCKET_NAME, S3_ADDRESS))
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
        server.start("servant_1",
            Tasks.TaskSpec.newBuilder()
                .setTid("task_1")
                .setZygote(Operations.Zygote.newBuilder()
                    .setName("zygote_1")
                    .addSlots(taskOutputSlot)
                    .setFuze("echo 'i-am-a-hacker' > /tmp/lzy_servant_1/slot_1 && echo 'hello'")
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

        server.waitTaskCompleted("servant_1", "task_1");
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
        System.out.println("\n----- PREPARE PORTAL FOR TASK 2, TASK 3 -----------------------------------------\n");

        ///// consumer tasks  /////

        // create channels for task_2, task_3
        createChannel("channel_2");
        createChannel("task_2:stdout");
        createChannel("task_2:stderr");
        createChannel("channel_3");
        createChannel("task_3:stdout");
        createChannel("task_3:stderr");

        // open portal output slot
        server.openPortalSlots(LzyPortalApi.OpenSlotsRequest.newBuilder()
            .addSlots(LzyPortalApi.PortalSlotDesc.newBuilder()
                .setSnapshot(makeAmazonSnapshot("snapshot_1", BUCKET_NAME, S3_ADDRESS))
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

        // open portal output slot
        server.openPortalSlots(LzyPortalApi.OpenSlotsRequest.newBuilder()
            .addSlots(LzyPortalApi.PortalSlotDesc.newBuilder()
                .setSnapshot(makeAmazonSnapshot("snapshot_1", BUCKET_NAME, S3_ADDRESS))
                .setSlot(makeOutputFileSlot("/slot_3"))
                .setChannelId("channel_3"))
            .addSlots(LzyPortalApi.PortalSlotDesc.newBuilder()
                .setSlot(makeInputPipeSlot("/portal_task_3:stdout"))
                .setChannelId("task_3:stdout")
                .setStdout(makeStdoutStorage("task_3"))
                .build())
            .addSlots(LzyPortalApi.PortalSlotDesc.newBuilder()
                .setSlot(makeInputPipeSlot("/portal_task_3:stderr"))
                .setChannelId("task_3:stderr")
                .setStderr(makeStderrStorage("task_3"))
                .build())
            .build());

        Thread.sleep(Duration.ofSeconds(1).toMillis());
        System.out.println("\n----- RUN TASK 2 & TASK 3 -----------------------------------------\n");

        var tmpFile2 = File.createTempFile("lzy", "test-result-2");
        tmpFile2.deleteOnExit();

        var tmpFile3 = File.createTempFile("lzy", "test-result-3");
        tmpFile3.deleteOnExit();

        var taskInputSlot2 = makeInputFileSlot("/slot_2");
        var taskInputSlot3 = makeInputFileSlot("/slot_3");

        // run task and load data from portal
        server.start("servant_2",
            Tasks.TaskSpec.newBuilder()
                .setTid("task_2")
                .setZygote(Operations.Zygote.newBuilder()
                    .setName("zygote_2")
                    .addSlots(taskInputSlot2)
                    .setFuze("/tmp/lzy_servant_2/sbin/cat /tmp/lzy_servant_2/slot_2 > "
                        + tmpFile2.getAbsolutePath())
                    .build())
                .addAssignments(Tasks.SlotAssignment.newBuilder()
                    .setTaskId("task_2")
                    .setSlot(taskInputSlot2)
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

        // run task and load data from portal
        server.start("servant_3",
            Tasks.TaskSpec.newBuilder()
                .setTid("task_3")
                .setZygote(Operations.Zygote.newBuilder()
                    .setName("zygote_3")
                    .addSlots(taskInputSlot3)
                    .setFuze("/tmp/lzy_servant_3/sbin/cat /tmp/lzy_servant_3/slot_3 > "
                        + tmpFile3.getAbsolutePath())
                    .build())
                .addAssignments(Tasks.SlotAssignment.newBuilder()
                    .setTaskId("task_3")
                    .setSlot(taskInputSlot3)
                    .setBinding("channel_3")
                    .build())
                .addAssignments(Tasks.SlotAssignment.newBuilder()
                    .setTaskId("task_3")
                    .setSlot(makeOutputPipeSlot("/dev/stdout"))
                    .setBinding("task_3:stdout")
                    .build())
                .addAssignments(Tasks.SlotAssignment.newBuilder()
                    .setTaskId("task_3")
                    .setSlot(makeOutputPipeSlot("/dev/stderr"))
                    .setBinding("task_3:stderr")
                    .build())
                .build(),
            SuccessStreamObserver.wrap(state -> System.out.println("Progress: " + JsonUtils.printSingleLine(state))));

        // wait
        server.waitTaskCompleted("servant_2", "task_2");
        server.waitPortalCompleted();

        // wait
        server.waitTaskCompleted("servant_3", "task_3");
        server.waitPortalCompleted();

        // task_3 clean up
        // task_2 clean up
        System.out.println("-- cleanup task_2 scenario --");
        destroyChannel("channel_2");
        destroyChannel("task_2:stdout");
        destroyChannel("task_2:stderr");

        System.out.println("-- cleanup task_3 scenario --");
        destroyChannel("channel_3");
        destroyChannel("task_3:stdout");
        destroyChannel("task_3:stderr");


        Thread.sleep(Duration.ofSeconds(1).toMillis());
        System.out.println("\n----------------------------------------------\n");

        destroyChannel("portal:stdout");
        destroyChannel("portal:stderr");

        var result2 = new String(Files.readAllBytes(tmpFile2.toPath()));
        var result3 = new String(Files.readAllBytes(tmpFile3.toPath()));
        Assert.assertEquals("i-am-a-hacker\n", result2);
        Assert.assertEquals("i-am-a-hacker\n", result3);
    }
}
