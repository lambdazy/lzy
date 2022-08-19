package ai.lzy.servant.portal;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.time.Duration;
import java.util.HashSet;

import static ai.lzy.test.GrpcUtils.makeInputFileSlot;
import static ai.lzy.test.GrpcUtils.makeOutputFileSlot;

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
        startPortal();

        var portalStdout = readPortalSlot("portal:stdout");
        var portalStderr = readPortalSlot("portal:stderr");

        // just for logs
        Thread.sleep(Duration.ofSeconds(1).toMillis());
        System.out.println("\n----- PREPARE PORTAL FOR TASK 1 -----------------------------------------\n");

        String firstServantId = preparePortalForTask(1, true, true, "snapshot_1");

        // just for logs
        Thread.sleep(Duration.ofSeconds(1).toMillis());
        System.out.println("\n----- RUN TASK 1 -----------------------------------------\n");

        var taskOutputSlot = makeOutputFileSlot("/slot_1");

        String firstTaskId = startTask(1, "echo 'i-am-a-hacker' > /tmp/lzy_servant_1/slot_1 && echo 'hello'",
            taskOutputSlot, null);
        server.waitTaskCompleted(firstServantId, firstTaskId);

        Assert.assertEquals(firstTaskId + "; hello\n", portalStdout.take());
        Assert.assertEquals(firstTaskId + "; ", portalStdout.take());
        Assert.assertEquals(firstTaskId + "; ", portalStderr.take());
        server.waitPortalCompleted();

        // task_1 clean up
        System.out.println("-- cleanup task1 scenario --");
        destroyChannel("channel_1");
        destroyChannel("task_1:stdout");
        destroyChannel("task_1:stderr");

        Thread.sleep(Duration.ofSeconds(1).toMillis());
        System.out.println("\n----- PREPARE PORTAL FOR TASK 2 -----------------------------------------\n");

        ///// consumer task  /////

        preparePortalForTask(2, false, false, "snapshot_1");

        Thread.sleep(Duration.ofSeconds(1).toMillis());
        System.out.println("\n----- RUN TASK 2 -----------------------------------------\n");

        var tmpFile = File.createTempFile("lzy", "test-result");
        tmpFile.deleteOnExit();

        var taskInputSlot = makeInputFileSlot("/slot_2");

        String secondTaskId = startTask(2, "echo 'x' && /tmp/lzy_servant_1/sbin/cat /tmp/lzy_servant_1/slot_2 > "
            + tmpFile.getAbsolutePath(), taskInputSlot, "servant_1");
        server.waitTaskCompleted(firstServantId, secondTaskId);

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
        startPortal();

        var portalStdout = readPortalSlot("portal:stdout");
        var portalStderr = readPortalSlot("portal:stderr");

        // just for logs
        Thread.sleep(Duration.ofSeconds(1).toMillis());
        System.out.println("\n----- PREPARE PORTAL FOR TASKS -----------------------------------------\n");

        String firstServantId = preparePortalForTask(1, true, true, "snapshot_1");
        String secondServantId = preparePortalForTask(2, true, true, "snapshot_2");

        // just for logs
        Thread.sleep(Duration.ofSeconds(1).toMillis());
        System.out.println("\n----- RUN TASKS -----------------------------------------\n");

        var task1OutputSlot = makeOutputFileSlot("/slot_1");
        var task2OutputSlot = makeOutputFileSlot("/slot_2");

        String firstTaskId = startTask(1, "echo 'hello from task_1' > /tmp/lzy_servant_1/slot_1 && "
            + "echo 'hello from task_1'", task1OutputSlot, null);
        String secondTaskId = startTask(2, "echo 'hello from task_2' > /tmp/lzy_servant_2/slot_2 && "
            + "echo 'hello from task_2'", task2OutputSlot, null);

        server.waitTaskCompleted(firstServantId, firstTaskId);
        server.waitTaskCompleted(secondServantId, secondTaskId);
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
        startPortal();

        var portalStdout = readPortalSlot("portal:stdout");
        var portalStderr = readPortalSlot("portal:stderr");

        // just for logs
        Thread.sleep(Duration.ofSeconds(1).toMillis());
        System.out.println("\n----- PREPARE PORTAL FOR TASK 1 -----------------------------------------\n");

        String firstServantId = preparePortalForTask(1, true, true, "snapshot_1");

        // just for logs
        Thread.sleep(Duration.ofSeconds(1).toMillis());
        System.out.println("\n----- RUN TASK 1 -----------------------------------------\n");

        var taskOutputSlot = makeOutputFileSlot("/slot_1");

        String firstTaskId = startTask(1, "echo 'i-am-a-hacker' > /tmp/lzy_servant_1/slot_1 && echo 'hello'",
            taskOutputSlot, null);
        server.waitTaskCompleted(firstServantId, firstTaskId);

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

        String secondServantId = preparePortalForTask(2, true, false, "snapshot_1");
        String thirdServantId = preparePortalForTask(3, true, false, "snapshot_1");

        Thread.sleep(Duration.ofSeconds(1).toMillis());
        System.out.println("\n----- RUN TASK 2 -----------------------------------------\n");

        var tmpFile2 = File.createTempFile("lzy", "test-result-2");
        tmpFile2.deleteOnExit();

        var taskInputSlot2 = makeInputFileSlot("/slot_2");

        String secondTaskId = startTask(2, "echo 'x' && /tmp/lzy_servant_2/sbin/cat /tmp/lzy_servant_2/slot_2 > "
            + tmpFile2.getAbsolutePath(), taskInputSlot2, null);
        server.waitTaskCompleted(secondServantId, secondTaskId);

        Assert.assertEquals("task_2; x\n", portalStdout.take());
        Assert.assertEquals("task_2; ", portalStdout.take());
        Assert.assertEquals("task_2; ", portalStderr.take());
        server.waitPortalCompleted();

        Thread.sleep(Duration.ofSeconds(1).toMillis());
        System.out.println("\n----- RUN TASK 3 -----------------------------------------\n");

        var tmpFile3 = File.createTempFile("lzy", "test-result-3");
        tmpFile3.deleteOnExit();

        var taskInputSlot3 = makeInputFileSlot("/slot_3");

        String thirdTaskId = startTask(3, "echo 'x' && /tmp/lzy_servant_3/sbin/cat /tmp/lzy_servant_3/slot_3 > "
            + tmpFile3.getAbsolutePath(), taskInputSlot3, null);
        server.waitTaskCompleted(thirdServantId, thirdTaskId);

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
        startPortal();

        var portalStdout = readPortalSlot("portal:stdout");
        var portalStderr = readPortalSlot("portal:stderr");

        // just for logs
        Thread.sleep(Duration.ofSeconds(1).toMillis());
        System.out.println("\n----- PREPARE PORTAL FOR TASK 1 -----------------------------------------\n");

        String firstServantId = preparePortalForTask(1, true, true, "snapshot_1");

        // just for logs
        Thread.sleep(Duration.ofSeconds(1).toMillis());
        System.out.println("\n----- RUN TASK 1 -----------------------------------------\n");

        var taskOutputSlot = makeOutputFileSlot("/slot_1");
        String firstTaskId = startTask(1, "echo 'i-am-a-hacker' > /tmp/lzy_servant_1/slot_1 && echo 'hello'",
            taskOutputSlot, null);
        server.waitTaskCompleted(firstServantId, firstTaskId);

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

        String secondServantId = preparePortalForTask(2, true, false, "snapshot_1");
        String thirdServantId = preparePortalForTask(3, true, false, "snapshot_1");

        Thread.sleep(Duration.ofSeconds(1).toMillis());
        System.out.println("\n----- RUN TASK 2 & TASK 3 -----------------------------------------\n");

        var tmpFile2 = File.createTempFile("lzy", "test-result-2");
        tmpFile2.deleteOnExit();

        var tmpFile3 = File.createTempFile("lzy", "test-result-3");
        tmpFile3.deleteOnExit();

        var taskInputSlot2 = makeInputFileSlot("/slot_2");
        var taskInputSlot3 = makeInputFileSlot("/slot_3");

        String secondTaskId = startTask(2, "/tmp/lzy_servant_2/sbin/cat /tmp/lzy_servant_2/slot_2 > "
            + tmpFile2.getAbsolutePath(), taskInputSlot2, null);
        String thirdTaskId = startTask(3, "/tmp/lzy_servant_3/sbin/cat /tmp/lzy_servant_3/slot_3 > "
            + tmpFile3.getAbsolutePath(), taskInputSlot3, null);

        // wait
        server.waitTaskCompleted(secondServantId, secondTaskId);
        server.waitPortalCompleted();

        // wait
        server.waitTaskCompleted(thirdServantId, thirdTaskId);
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
