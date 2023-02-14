package ai.lzy.portal;

import ai.lzy.portal.slots.StorageClients;
import ai.lzy.test.GrpcUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.time.Duration;
import java.util.HashSet;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static ai.lzy.test.GrpcUtils.makeInputFileSlot;
import static ai.lzy.test.GrpcUtils.makeOutputFileSlot;

public class PortalCommonTest extends PortalTestBase {
    @Test
    public void testPortalSnapshotWithLongSlotName() throws InterruptedException, IOException {
        var portalStdout = readPortalSlot("portal:stdout");
        var portalStderr = readPortalSlot("portal:stderr");

        System.out.println("\n----- PREPARE PORTAL FOR TASK 1 -----------------------------------------\n");

        String firstWorkerId = prepareTask(1, true, true, "snapshot1");
        String slotName = IntStream.range(0, 100).boxed().map(Objects::toString)
            .collect(Collectors.joining("_", "/slot_", "_1"));

        System.out.println("\n----- RUN TASK 1 -----------------------------------------\n");

        var taskOutputSlot = makeOutputFileSlot(slotName);

        String firstTaskId = startTask(1, ("echo 'i-am-a-hacker' > /tmp/lzy_worker_1" +
            "%s && echo 'hello'").formatted(slotName), taskOutputSlot, null);

        Assert.assertEquals("[LZY-REMOTE-" + firstTaskId + "] - hello\n", portalStdout.take());
        Assert.assertTrue(portalStdout.isEmpty());
        Assert.assertTrue(portalStderr.isEmpty());
        waitPortalCompleted();
        finishPortal();

        var storageConfig = GrpcUtils.makeAmazonSnapshot("snapshot1", BUCKET_NAME, S3_ADDRESS).getStorageConfig();
        var s3client = StorageClients.provider(storageConfig).get(Executors.newFixedThreadPool(5));
        var tempfile = File.createTempFile("portal_", "_test");
        tempfile.deleteOnExit();
        s3client.read(URI.create(storageConfig.getUri()), tempfile.toPath());
        String[] content = {null};
        try (var reader = new BufferedReader(new FileReader(tempfile))) {
            content[0] = reader.readLine();
        }
        Assert.assertEquals("i-am-a-hacker", content[0]);

        // task_1 clean up
        System.out.println("-- cleanup task1 scenario --");
        destroyChannel("channel_1");
        destroyChannel("task_1:stdout");
        destroyChannel("task_1:stderr");
    }

    @Test
    public void testSnapshotStoredToS3() throws Exception {
        var portalStdout = readPortalSlot("portal:stdout");
        var portalStderr = readPortalSlot("portal:stderr");

        System.out.println("\n----- PREPARE PORTAL FOR TASK 1 -----------------------------------------\n");

        String firstWorkerId = prepareTask(1, true, true, "snapshot1");

        System.out.println("\n----- RUN TASK 1 -----------------------------------------\n");

        var taskOutputSlot = makeOutputFileSlot("/slot_1");

        String firstTaskId = startTask(1, "echo 'i-am-a-hacker' > /tmp/lzy_worker_1/slot_1 && echo 'hello'",
            taskOutputSlot, null);

        Assert.assertEquals("[LZY-REMOTE-" + firstTaskId + "] - hello\n", portalStdout.take());
        Assert.assertTrue(portalStdout.isEmpty());
        Assert.assertTrue(portalStderr.isEmpty());
        waitPortalCompleted();
        finishPortal();

        var storageConfig = GrpcUtils.makeAmazonSnapshot("snapshot1", BUCKET_NAME, S3_ADDRESS).getStorageConfig();
        var s3client = StorageClients.provider(storageConfig).get(Executors.newFixedThreadPool(5));
        var tempfile = File.createTempFile("portal_", "_test");
        tempfile.deleteOnExit();
        s3client.read(URI.create(storageConfig.getUri()), tempfile.toPath());
        String[] content = {null};
        try (var reader = new BufferedReader(new FileReader(tempfile))) {
            content[0] = reader.readLine();
        }
        Assert.assertEquals("i-am-a-hacker", content[0]);

        // task_1 clean up
        System.out.println("-- cleanup task1 scenario --");
        destroyChannel("channel_1");
        destroyChannel("task_1:stdout");
        destroyChannel("task_1:stderr");
    }

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

        Assert.assertEquals("[LZY-REMOTE-" + firstTaskId + "] - hello\n", portalStdout.take());
        Assert.assertTrue(portalStdout.isEmpty());
        Assert.assertTrue(portalStderr.isEmpty());
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

        Assert.assertEquals("[LZY-REMOTE-" + secondTaskId + "] - x\n", portalStdout.take());
        Assert.assertTrue(portalStdout.isEmpty());
        Assert.assertTrue(portalStderr.isEmpty());
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
        Assert.assertTrue(portalStderr.isEmpty());
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

        Assert.assertEquals("[LZY-REMOTE-" + firstTaskId + "] - hello\n", portalStdout.take());
        Assert.assertTrue(portalStdout.isEmpty());
        Assert.assertTrue(portalStderr.isEmpty());
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

        Assert.assertEquals("[LZY-REMOTE-" + secondTaskId + "] - x\n", portalStdout.take());
        Assert.assertTrue(portalStdout.isEmpty());
        Assert.assertTrue(portalStderr.isEmpty());
        waitPortalCompleted();

        System.out.println("\n----- RUN TASK 3 -----------------------------------------\n");

        var tmpFile3 = File.createTempFile("lzy", "test-result-3");
        tmpFile3.deleteOnExit();

        var taskInputSlot3 = makeInputFileSlot("/slot_3");

        String thirdTaskId = startTask(3, "echo 'x' && /tmp/lzy_worker_3/sbin/cat /tmp/lzy_worker_3/slot_3 > "
            + tmpFile3.getAbsolutePath(), taskInputSlot3, null);

        Assert.assertEquals("[LZY-REMOTE-" + thirdTaskId + "] - x\n", portalStdout.take());
        Assert.assertTrue(portalStdout.isEmpty());
        Assert.assertTrue(portalStderr.isEmpty());
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

        Assert.assertEquals("[LZY-REMOTE-" + firstTaskId + "] - hello\n", portalStdout.take());
        Assert.assertTrue(portalStdout.isEmpty());
        Assert.assertTrue(portalStderr.isEmpty());
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
        waitPortalCompleted();

        // wait
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
