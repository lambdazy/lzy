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
import java.util.HashSet;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


public class PortalCommonTest extends PortalTestBase {
    @Test
    public void testPortalSnapshotWithLongSlotName() throws InterruptedException, IOException {
        var portalStdout = readPortalSlot("portal:stdout");
        var portalStderr = readPortalSlot("portal:stderr");

        System.out.println("\n----- PREPARE PORTAL FOR TASK 1 -----------------------------------------\n");

        String slotName = IntStream.range(0, 100).boxed().map(Objects::toString)
            .collect(Collectors.joining("_", "/slot_", "_1"));

        var snapshotId = "snapshot_" + UUID.randomUUID();

        try (var worker = startWorker()) {
            System.out.println("\n----- RUN TASK 1 -----------------------------------------\n");

            String firstTaskId = startTask(("echo 'i-am-a-hacker' > $LZY_MOUNT" +
                "%s && echo 'hello'").formatted(slotName), slotName, worker, true, snapshotId);

            Assert.assertEquals("[LZY-REMOTE-" + firstTaskId + "] - hello\n", portalStdout.take());
            Assert.assertTrue(portalStdout.isEmpty());
            Assert.assertTrue(portalStderr.isEmpty());
            waitPortalCompleted();
            finishPortal();

            var storageConfig = GrpcUtils.makeAmazonSnapshot(snapshotId, BUCKET_NAME, S3_ADDRESS).getStorageConfig();
            var s3client = StorageClients.provider(storageConfig).get(Executors.newFixedThreadPool(5));
            var tempfile = File.createTempFile("portal_", "_test");
            tempfile.deleteOnExit();
            s3client.read(URI.create(storageConfig.getUri()), tempfile.toPath());
            String[] content = {null};
            try (var reader = new BufferedReader(new FileReader(tempfile))) {
                content[0] = reader.readLine();
            }
            Assert.assertEquals("i-am-a-hacker", content[0]);
        }

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

        try (var worker = startWorker()) {

            System.out.println("\n----- RUN TASK 1 -----------------------------------------\n");

            var taskOutputSlot = "/" + UUID.randomUUID();
            var snapshotId = "snapshot_" + UUID.randomUUID();

            String firstTaskId = startTask("echo 'i-am-a-hacker' > $LZY_MOUNT/%s && echo 'hello'"
                .formatted(taskOutputSlot), taskOutputSlot, worker, true, snapshotId);

            Assert.assertEquals("[LZY-REMOTE-" + firstTaskId + "] - hello\n", portalStdout.take());
            Assert.assertTrue(portalStdout.isEmpty());
            Assert.assertTrue(portalStderr.isEmpty());
            waitPortalCompleted();
            finishPortal();

            var storageConfig = GrpcUtils.makeAmazonSnapshot(snapshotId, BUCKET_NAME, S3_ADDRESS).getStorageConfig();
            var s3client = StorageClients.provider(storageConfig).get(Executors.newFixedThreadPool(5));
            var tempfile = File.createTempFile("portal_", "_test");
            tempfile.deleteOnExit();
            s3client.read(URI.create(storageConfig.getUri()), tempfile.toPath());
            String[] content = {null};
            try (var reader = new BufferedReader(new FileReader(tempfile))) {
                content[0] = reader.readLine();
            }
            Assert.assertEquals("i-am-a-hacker", content[0]);
        }

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
        var tmpFile = File.createTempFile("lzy", "test-result");
        tmpFile.deleteOnExit();
        var snapshotId = "snapshot_" + UUID.randomUUID();

        try (var worker = startWorker()) {

            System.out.println("\n----- RUN TASK 1 -----------------------------------------\n");

            var taskOutputSlot = "/" + UUID.randomUUID();

            String firstTaskId = startTask("echo 'i-am-a-hacker' > $LZY_MOUNT%s && echo 'hello'"
                .formatted(taskOutputSlot),
                taskOutputSlot, worker, true, snapshotId);

            Assert.assertEquals("[LZY-REMOTE-" + firstTaskId + "] - hello\n", portalStdout.take());
            Assert.assertTrue(portalStdout.isEmpty());
            Assert.assertTrue(portalStderr.isEmpty());
            waitPortalCompleted();

            // task_1 clean up
            System.out.println("-- cleanup task1 scenario --");
            destroyChannel("channel_1");
            destroyChannel("task_1:stdout");
            destroyChannel("task_1:stderr");

            System.out.println("\n----- RUN TASK 2 -----------------------------------------\n");

            var taskInputSlot = "/" + UUID.randomUUID();

            String secondTaskId = startTask("echo 'x' && $LZY_MOUNT/sbin/cat $LZY_MOUNT%s > ".formatted(taskInputSlot)
                + tmpFile.getAbsolutePath(), taskInputSlot, worker, false, snapshotId);

            Assert.assertEquals("[LZY-REMOTE-" + secondTaskId + "] - x\n", portalStdout.take());
            Assert.assertTrue(portalStdout.isEmpty());
            Assert.assertTrue(portalStderr.isEmpty());
            waitPortalCompleted();
        }

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
        String firstTaskId;
        String secondTaskId;
        var snapshotId1 = "snapshot_" + UUID.randomUUID();
        var snapshotId2 = "snapshot_" + UUID.randomUUID();

        try (var worker1 = startWorker()) {

            System.out.println("\n----- RUN TASKS -----------------------------------------\n");

            var task1OutputSlot = "/" + UUID.randomUUID();
            var task2OutputSlot = "/" + UUID.randomUUID();

            firstTaskId = startTask("echo 'hello from task_1' > $LZY_MOUNT/%s && ".formatted(task1OutputSlot)
                + "echo 'hello from task_1'", task1OutputSlot, worker1, true, snapshotId1);
            secondTaskId = startTask("echo 'hello from task_2' > $LZY_MOUNT/%s && ".formatted(task2OutputSlot)
                + "echo 'hello from task_2'", task2OutputSlot, worker1, true, snapshotId2);
            waitPortalCompleted();
        }

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

        System.out.println("\n----- RUN TASK 1 -----------------------------------------\n");

        var tmpFile2 = File.createTempFile("lzy", "test-result-2");
        var tmpFile3 = File.createTempFile("lzy", "test-result-3");
        var snapshotId = "snapshot_" + UUID.randomUUID();

        try (var worker1 = startWorker()) {

            var taskOutputSlot = "/" + UUID.randomUUID();

            String firstTaskId = startTask("echo 'i-am-a-hacker' > $LZY_MOUNT/%s && echo 'hello'"
                .formatted(taskOutputSlot), taskOutputSlot, worker1, true, snapshotId);

            Assert.assertEquals("[LZY-REMOTE-" + firstTaskId + "] - hello\n", portalStdout.take());
            Assert.assertTrue(portalStdout.isEmpty());
            Assert.assertTrue(portalStderr.isEmpty());
            waitPortalCompleted();

            // task_1 clean up
            System.out.println("-- cleanup task1 scenario --");
            destroyChannel("channel_1");
            destroyChannel("task_1:stdout");
            destroyChannel("task_1:stderr");

            System.out.println("\n----- RUN TASK 2 -----------------------------------------\n");
            tmpFile2.deleteOnExit();

            var taskInputSlot2 = "/" + UUID.randomUUID();

            String secondTaskId = startTask("echo 'x' && $LZY_MOUNT/sbin/cat $LZY_MOUNT/%s > ".formatted(taskInputSlot2)
                .formatted(taskInputSlot2) + tmpFile2.getAbsolutePath(), taskInputSlot2, worker1, false, snapshotId);

            Assert.assertEquals("[LZY-REMOTE-" + secondTaskId + "] - x\n", portalStdout.take());
            Assert.assertTrue(portalStdout.isEmpty());
            Assert.assertTrue(portalStderr.isEmpty());
            waitPortalCompleted();

            System.out.println("\n----- RUN TASK 3 -----------------------------------------\n");


            tmpFile3.deleteOnExit();

            var taskInputSlot3 = "/" + UUID.randomUUID();

            String thirdTaskId = startTask("echo 'x' && $LZY_MOUNT/sbin/cat $LZY_MOUNT/%s > ".formatted(taskInputSlot3)
                + tmpFile3.getAbsolutePath(), taskInputSlot3, worker1, false, snapshotId);

            Assert.assertEquals("[LZY-REMOTE-" + thirdTaskId + "] - x\n", portalStdout.take());
            Assert.assertTrue(portalStdout.isEmpty());
            Assert.assertTrue(portalStderr.isEmpty());
            waitPortalCompleted();
        }

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

        var snapshotId = "snapshot_" + UUID.randomUUID();

        System.out.println("\n----- PREPARE PORTAL FOR TASK 1 -----------------------------------------\n");
        var tmpFile2 = File.createTempFile("lzy", "test-result-2");
        tmpFile2.deleteOnExit();

        var tmpFile3 = File.createTempFile("lzy", "test-result-3");
        tmpFile3.deleteOnExit();

        try (var worker1 = startWorker()) {

            System.out.println("\n----- RUN TASK 1 -----------------------------------------\n");

            var taskOutputSlot = "/" + UUID.randomUUID();
            String firstTaskId = startTask("echo 'i-am-a-hacker' > $LZY_MOUNT/%s && echo 'hello'"
                .formatted(taskOutputSlot), taskOutputSlot, worker1, true, snapshotId);

            Assert.assertEquals("[LZY-REMOTE-" + firstTaskId + "] - hello\n", portalStdout.take());
            Assert.assertTrue(portalStdout.isEmpty());
            Assert.assertTrue(portalStderr.isEmpty());
            waitPortalCompleted();

            // task_1 clean up
            System.out.println("-- cleanup task1 scenario --");
            destroyChannel("channel_1");
            destroyChannel("task_1:stdout");
            destroyChannel("task_1:stderr");

            System.out.println("\n----- RUN TASK 2 & TASK 3 -----------------------------------------\n");

            var taskInputSlot2 = "/" + UUID.randomUUID();
            var taskInputSlot3 = "/" + UUID.randomUUID();

            startTask("$LZY_MOUNT/sbin/cat $LZY_MOUNT/%s > ".formatted(taskInputSlot2)
                + tmpFile2.getAbsolutePath(), taskInputSlot2, worker1, false, snapshotId);
            startTask("$LZY_MOUNT/sbin/cat $LZY_MOUNT/%s > ".formatted(taskInputSlot3)
                + tmpFile3.getAbsolutePath(), taskInputSlot3, worker1, false, snapshotId);

            // wait
            waitPortalCompleted();

            // wait
            waitPortalCompleted();
        }

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
