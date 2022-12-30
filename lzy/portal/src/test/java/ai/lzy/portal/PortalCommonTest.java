package ai.lzy.portal;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.time.Duration;
import java.util.HashSet;

import static ai.lzy.test.GrpcUtils.makeInputFileSlot;
import static ai.lzy.test.GrpcUtils.makeOutputFileSlot;

public class PortalCommonTest extends PortalTestBase {
    @Test
    public void makeSnapshotOnPortalThenReadIt() throws Exception {
        var portalStdout = readPortalSlot("portal:stdout");
        var portalStderr = readPortalSlot("portal:stderr");

        System.out.println("\n----- PREPARE PORTAL FOR TASK 1 -----------------------------------------\n");

        String firstWorkerId = prepareTask(1, true, true, "snapshot_1");

        System.out.println("\n----- RUN TASK 1 -----------------------------------------\n");

        var taskOutputSlot = makeOutputFileSlot("/slot_1");

        String firstTaskId = startTask(1, "echo 'i-am-a-hacker' > /tmp/lzy_worker_1/slot_1 && echo 'hello'",
            taskOutputSlot, null);
        mocksServer.getSchedulerMock().awaitProcessing(firstWorkerId);

        Assert.assertEquals("[LZY-REMOTE-" + firstTaskId + "] - hello\n", portalStdout.take());
        Assert.assertEquals("", portalStdout.take());
        Assert.assertEquals("", portalStderr.take());
        waitPortalCompleted();

        // task_1 clean up
        System.out.println("-- cleanup task1 scenario --");
        destroyChannel("channel_1");
        destroyChannel("task_1:stdout");
        destroyChannel("task_1:stderr");

        Thread.sleep(Duration.ofSeconds(1).toMillis());
        System.out.println("\n----- PREPARE PORTAL FOR TASK 2 -----------------------------------------\n");

        ///// consumer task  /////

        prepareTask(2, false, false, "snapshot_1");

        Thread.sleep(Duration.ofSeconds(1).toMillis());
        System.out.println("\n----- RUN TASK 2 -----------------------------------------\n");

        var tmpFile = File.createTempFile("lzy", "test-result");
        tmpFile.deleteOnExit();

        var taskInputSlot = makeInputFileSlot("/slot_2");

        String secondTaskId = startTask(2, "echo 'x' && /tmp/lzy_worker_1/sbin/cat /tmp/lzy_worker_1/slot_2 > "
            + tmpFile.getAbsolutePath(), taskInputSlot, "worker_1");
        mocksServer.getSchedulerMock().awaitProcessing(firstWorkerId);

        Assert.assertEquals("[LZY-REMOTE-" + secondTaskId + "] - x\n", portalStdout.take());
        Assert.assertEquals("", portalStdout.take());
        Assert.assertEquals("", portalStderr.take());
        waitPortalCompleted();

        // task_2 clean up
        System.out.println("-- cleanup task2 scenario --");
        destroyChannel("channel_2");
        destroyChannel("task_2:stdout");
        destroyChannel("task_2:stderr");

        Assert.assertTrue(portalStdout.isEmpty());
        Assert.assertTrue(portalStderr.isEmpty());

        var result = new String(Files.readAllBytes(tmpFile.toPath()));
        Assert.assertEquals("i-am-a-hacker\n", result);
    }

    @Test
    public void multipleTasksMakeSnapshots() throws Exception {
        var portalStdout = readPortalSlot("portal:stdout");
        var portalStderr = readPortalSlot("portal:stderr");

        System.out.println("\n----- PREPARE PORTAL FOR TASKS -----------------------------------------\n");

        String firstWorkerId = prepareTask(1, true, true, "snapshot_1");
        String secondWorkerId = prepareTask(2, true, true, "snapshot_2");

        System.out.println("\n----- RUN TASKS -----------------------------------------\n");

        var task1OutputSlot = makeOutputFileSlot("/slot_1");
        var task2OutputSlot = makeOutputFileSlot("/slot_2");

        String firstTaskId = startTask(1, "echo 'hello from task_1' > /tmp/lzy_worker_1/slot_1 && "
            + "echo 'hello from task_1'", task1OutputSlot, null);
        String secondTaskId = startTask(2, "echo 'hello from task_2' > /tmp/lzy_worker_2/slot_2 && "
            + "echo 'hello from task_2'", task2OutputSlot, null);

        mocksServer.getSchedulerMock().awaitProcessing(firstWorkerId);
        mocksServer.getSchedulerMock().awaitProcessing(secondWorkerId);
        waitPortalCompleted();

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
                add("[LZY-REMOTE-" + firstTaskId + "] - hello from task_1\n");
                add("[LZY-REMOTE-" + secondTaskId + "] - hello from task_2\n");
            }
        };

        while (!expected.isEmpty()) {
            var actual = portalStdout.take();
            Assert.assertTrue(actual.toString(), actual instanceof String);
            Assert.assertTrue(actual.toString(), expected.remove(actual));
        }
        Assert.assertNull(portalStdout.poll());

        expected.add(firstTaskId + "; ");
        expected.add(secondTaskId + "; ");

        while (!expected.isEmpty()) {
            var actual = portalStderr.take();
            Assert.assertTrue(actual.toString(), actual instanceof String);
            Assert.assertTrue(actual.toString(), expected.remove(actual));
        }
        Assert.assertNull(portalStderr.poll());
    }

    @Test
    public void multipleSequentialConsumerAndSingleSnapshotProducer() throws Exception {
        var portalStdout = readPortalSlot("portal:stdout");
        var portalStderr = readPortalSlot("portal:stderr");

        System.out.println("\n----- PREPARE PORTAL FOR TASK 1 -----------------------------------------\n");

        String firstWorkerId = prepareTask(1, true, true, "snapshot_1");

        System.out.println("\n----- RUN TASK 1 -----------------------------------------\n");

        var taskOutputSlot = makeOutputFileSlot("/slot_1");

        String firstTaskId = startTask(1, "echo 'i-am-a-hacker' > /tmp/lzy_worker_1/slot_1 && echo 'hello'",
            taskOutputSlot, null);
        mocksServer.getSchedulerMock().awaitProcessing(firstWorkerId);

        Assert.assertEquals("[LZY-REMOTE-" + firstTaskId + "] - hello\n", portalStdout.take());
        Assert.assertEquals("", portalStdout.take());
        Assert.assertEquals("", portalStderr.take());
        waitPortalCompleted();

        // task_1 clean up
        System.out.println("-- cleanup task1 scenario --");
        destroyChannel("channel_1");
        destroyChannel("task_1:stdout");
        destroyChannel("task_1:stderr");

        System.out.println("\n----- PREPARE PORTAL FOR TASK 2, TASK 3 -----------------------------------------\n");

        ///// consumer tasks  /////

        String secondWorkerId = prepareTask(2, true, false, "snapshot_1");
        String thirdWorkerId = prepareTask(3, true, false, "snapshot_1");

        System.out.println("\n----- RUN TASK 2 -----------------------------------------\n");

        var tmpFile2 = File.createTempFile("lzy", "test-result-2");
        tmpFile2.deleteOnExit();

        var taskInputSlot2 = makeInputFileSlot("/slot_2");

        String secondTaskId = startTask(2, "echo 'x' && /tmp/lzy_worker_2/sbin/cat /tmp/lzy_worker_2/slot_2 > "
            + tmpFile2.getAbsolutePath(), taskInputSlot2, null);
        mocksServer.getSchedulerMock().awaitProcessing(secondWorkerId);

        Assert.assertEquals("[LZY-REMOTE-" + secondTaskId + "] - x\n", portalStdout.take());
        Assert.assertEquals("", portalStdout.take());
        Assert.assertEquals("", portalStderr.take());
        waitPortalCompleted();

        System.out.println("\n----- RUN TASK 3 -----------------------------------------\n");

        var tmpFile3 = File.createTempFile("lzy", "test-result-3");
        tmpFile3.deleteOnExit();

        var taskInputSlot3 = makeInputFileSlot("/slot_3");

        String thirdTaskId = startTask(3, "echo 'x' && /tmp/lzy_worker_3/sbin/cat /tmp/lzy_worker_3/slot_3 > "
            + tmpFile3.getAbsolutePath(), taskInputSlot3, null);
        mocksServer.getSchedulerMock().awaitProcessing(thirdWorkerId);

        Assert.assertEquals("[LZY-REMOTE-" + thirdTaskId + "] - x\n", portalStdout.take());
        Assert.assertEquals("", portalStdout.take());
        Assert.assertEquals("", portalStderr.take());
        waitPortalCompleted();

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

        Assert.assertTrue(portalStdout.isEmpty());
        Assert.assertTrue(portalStderr.isEmpty());

        var result2 = new String(Files.readAllBytes(tmpFile2.toPath()));
        var result3 = new String(Files.readAllBytes(tmpFile3.toPath()));
        Assert.assertEquals("i-am-a-hacker\n", result2);
        Assert.assertEquals("i-am-a-hacker\n", result3);
    }

    @Test
    public void multipleConcurrentConsumerAndSingleSnapshotProducer() throws Exception {
        var portalStdout = readPortalSlot("portal:stdout");
        var portalStderr = readPortalSlot("portal:stderr");

        System.out.println("\n----- PREPARE PORTAL FOR TASK 1 -----------------------------------------\n");

        String firstWorkerId = prepareTask(1, true, true, "snapshot_1");

        System.out.println("\n----- RUN TASK 1 -----------------------------------------\n");

        var taskOutputSlot = makeOutputFileSlot("/slot_1");
        String firstTaskId = startTask(1, "echo 'i-am-a-hacker' > /tmp/lzy_worker_1/slot_1 && echo 'hello'",
            taskOutputSlot, null);
        mocksServer.getSchedulerMock().awaitProcessing(firstWorkerId);

        Assert.assertEquals("[LZY-REMOTE-" + firstTaskId + "] - hello\n", portalStdout.take());
        Assert.assertEquals("", portalStdout.take());
        Assert.assertEquals("", portalStderr.take());
        waitPortalCompleted();

        // task_1 clean up
        System.out.println("-- cleanup task1 scenario --");
        destroyChannel("channel_1");
        destroyChannel("task_1:stdout");
        destroyChannel("task_1:stderr");

        System.out.println("\n----- PREPARE PORTAL FOR TASK 2, TASK 3 -----------------------------------------\n");

        ///// consumer tasks  /////

        String secondWorkerId = prepareTask(2, true, false, "snapshot_1");
        String thirdWorkerId = prepareTask(3, true, false, "snapshot_1");

        System.out.println("\n----- RUN TASK 2 & TASK 3 -----------------------------------------\n");

        var tmpFile2 = File.createTempFile("lzy", "test-result-2");
        tmpFile2.deleteOnExit();

        var tmpFile3 = File.createTempFile("lzy", "test-result-3");
        tmpFile3.deleteOnExit();

        var taskInputSlot2 = makeInputFileSlot("/slot_2");
        var taskInputSlot3 = makeInputFileSlot("/slot_3");

        startTask(2, "/tmp/lzy_worker_2/sbin/cat /tmp/lzy_worker_2/slot_2 > "
            + tmpFile2.getAbsolutePath(), taskInputSlot2, null);
        startTask(3, "/tmp/lzy_worker_3/sbin/cat /tmp/lzy_worker_3/slot_3 > "
            + tmpFile3.getAbsolutePath(), taskInputSlot3, null);

        // wait
        mocksServer.getSchedulerMock().awaitProcessing(secondWorkerId);
        waitPortalCompleted();

        // wait
        mocksServer.getSchedulerMock().awaitProcessing(thirdWorkerId);
        waitPortalCompleted();

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

        var result2 = new String(Files.readAllBytes(tmpFile2.toPath()));
        var result3 = new String(Files.readAllBytes(tmpFile3.toPath()));
        Assert.assertEquals("i-am-a-hacker\n", result2);
        Assert.assertEquals("i-am-a-hacker\n", result3);
    }
}
