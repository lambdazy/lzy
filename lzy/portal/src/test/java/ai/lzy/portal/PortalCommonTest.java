package ai.lzy.portal;

import ai.lzy.storage.StorageClientFactory;
import ai.lzy.test.GrpcUtils;
import com.amazonaws.services.s3.model.ObjectMetadata;
import io.findify.s3mock.provider.InMemoryProvider;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.URI;
import java.nio.file.Files;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static ai.lzy.util.kafka.test.KafkaTestUtils.StdlogMessage;
import static ai.lzy.util.kafka.test.KafkaTestUtils.assertStdLogs;

public class PortalCommonTest extends PortalTestBase {

    private StorageClientFactory storageClientFactory;

    @Override
    @Before
    public void before() throws Exception {
        super.before();
        storageClientFactory = new StorageClientFactory(10, 10);
    }

    @Override
    @After
    public void after() throws InterruptedException {
        super.after();
        storageClientFactory.destroy();
    }

    @Test
    public void testPortalSnapshotWithLongSlotName() throws Exception {
        System.out.println("\n----- PREPARE PORTAL FOR TASK 1 -----------------------------------------\n");

        String slotName = IntStream.range(0, 100).boxed().map(Objects::toString)
            .collect(Collectors.joining("_", "/slot_", "_1"));

        var snapshotId = idGenerator.generate("snapshot-");

        try (var worker = startWorker()) {
            System.out.println("\n----- RUN TASK 1 -----------------------------------------\n");

            String firstTaskId = startTask(
                ("echo 'i-am-a-hacker' > $LZY_MOUNT%s && echo 'hello'").formatted(slotName),
                slotName, worker, true, snapshotId, stdlogsTopic);

            assertStdLogs(stdlogs, List.of(StdlogMessage.out(firstTaskId, "hello")), List.of());
            waitPortalCompleted();
            finishPortal();
            finishStdlogsReader.finish();

            var storageConfig = GrpcUtils.makeAmazonSnapshot(snapshotId, BUCKET_NAME, S3_ADDRESS).getStorageConfig();
            var s3client = storageClientFactory.provider(storageConfig).get();
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
    }

    @Test
    public void testSnapshotStoredToS3() throws Exception {
        try (var worker = startWorker()) {
            System.out.println("\n----- RUN TASK 1 -----------------------------------------\n");

            var taskOutputSlot = "/" + idGenerator.generate();
            var snapshotId = idGenerator.generate("snapshot-");

            String firstTaskId = startTask(
                "echo 'i-am-a-hacker' > $LZY_MOUNT/%s && echo 'hello'".formatted(taskOutputSlot),
                taskOutputSlot, worker, true, snapshotId, stdlogsTopic);

            assertStdLogs(stdlogs, List.of(StdlogMessage.out(firstTaskId, "hello")), List.of());
            waitPortalCompleted();
            finishPortal();
            finishStdlogsReader.finish();

            var storageConfig = GrpcUtils.makeAmazonSnapshot(snapshotId, BUCKET_NAME, S3_ADDRESS).getStorageConfig();
            var s3client = storageClientFactory.provider(storageConfig).get();
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
    }

    @Test
    public void testSnapshotStoredToS3TooLong() throws Exception {
        var latch = new CountDownLatch(1);

        stopS3();
        startS3(new InMemoryProvider() {
            @Override
            public void putObject(String bucket, String key, byte[] data, ObjectMetadata objectMetadata) {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                super.putObject(bucket, key, data, objectMetadata);
            }
        });

        try (var worker = startWorker()) {
            System.out.println("\n----- RUN TASK 1 -----------------------------------------\n");

            var taskOutputSlot = "/" + idGenerator.generate();
            var snapshotId = idGenerator.generate("snapshot-");

            String firstTaskId = startTask(
                "echo 'i-am-a-hacker' > $LZY_MOUNT/%s && echo 'hello'".formatted(taskOutputSlot),
                taskOutputSlot, worker, true, snapshotId, stdlogsTopic);

            assertStdLogs(stdlogs, List.of(StdlogMessage.out(firstTaskId, "hello")), List.of());

            var portalCompleted = waitPortalCompleted(Duration.ofSeconds(3));
            Assert.assertFalse(portalCompleted);

            latch.countDown();
            waitPortalCompleted();

            finishPortal();
            finishStdlogsReader.finish();

            var storageConfig = GrpcUtils.makeAmazonSnapshot(snapshotId, BUCKET_NAME, S3_ADDRESS).getStorageConfig();
            var s3client = storageClientFactory.provider(storageConfig).get();
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
    }

    @Test
    public void makeSnapshotOnPortalThenReadIt() throws Exception {
        System.out.println("\n----- PREPARE PORTAL FOR TASK 1 -----------------------------------------\n");
        var tmpFile = File.createTempFile("lzy", "test-result");
        tmpFile.deleteOnExit();
        var snapshotId = idGenerator.generate("snapshot-");

        try (var worker = startWorker()) {
            System.out.println("\n----- RUN TASK 1 -----------------------------------------\n");
            var taskOutputSlot = idGenerator.generate("/");

            String firstTaskId = startTask(
                "echo 'i-am-a-hacker' > $LZY_MOUNT%s && echo 'hello'".formatted(taskOutputSlot),
                taskOutputSlot, worker, true, snapshotId, stdlogsTopic);

            assertStdLogs(stdlogs, List.of(StdlogMessage.out(firstTaskId, "hello")), List.of());
            waitPortalCompleted();

            // task_1 clean up
            System.out.println("-- cleanup task1 scenario --");
            destroyChannel("channel_1");

            System.out.println("\n----- RUN TASK 2 -----------------------------------------\n");
            var taskInputSlot = idGenerator.generate("/");

            String secondTaskId = startTask(
                "echo 'x' && $LZY_MOUNT/sbin/cat $LZY_MOUNT%s > %s".formatted(taskInputSlot, tmpFile.getAbsolutePath()),
                taskInputSlot, worker, false, snapshotId, stdlogsTopic);

            assertStdLogs(stdlogs, List.of(StdlogMessage.out(secondTaskId, "x")), List.of());
            waitPortalCompleted();
            finishStdlogsReader.finish();
        }

        // task_2 clean up
        System.out.println("-- cleanup task2 scenario --");
        destroyChannel("channel_2");

        var result = new String(Files.readAllBytes(tmpFile.toPath()));
        Assert.assertEquals("i-am-a-hacker\n", result);
    }

    @Test
    public void multipleTasksMakeSnapshots() throws Exception {
        System.out.println("\n----- PREPARE PORTAL FOR TASKS -----------------------------------------\n");
        String firstTaskId;
        String secondTaskId;
        var snapshotId1 = idGenerator.generate("snapshot-");
        var snapshotId2 = idGenerator.generate("snapshot-");

        try (var worker1 = startWorker()) {
            System.out.println("\n----- RUN TASKS -----------------------------------------\n");

            var task1OutputSlot = idGenerator.generate("/");
            var task2OutputSlot = idGenerator.generate("/");

            firstTaskId = startTask(
                "echo 'hello from task_1' > $LZY_MOUNT/%s && echo 'hello from task_1'".formatted(task1OutputSlot),
                task1OutputSlot, worker1, true, snapshotId1, stdlogsTopic);

            secondTaskId = startTask(
                "echo 'hello from task_2' > $LZY_MOUNT/%s && echo 'hello from task_2'".formatted(task2OutputSlot),
                task2OutputSlot, worker1, true, snapshotId2, stdlogsTopic);

            waitPortalCompleted();
        }

        System.out.println("\n----- TASKS DONE -----------------------------------------\n");

        System.out.println("-- cleanup tasks --");
        destroyChannel("channel_1");
        destroyChannel("channel_2");

        assertStdLogs(
            stdlogs,
            List.of(
                StdlogMessage.out(firstTaskId, "hello from task_1"),
                StdlogMessage.out(secondTaskId, "hello from task_2")),
            List.of());
        finishStdlogsReader.finish();
    }

    @Test
    public void multipleSequentialConsumerAndSingleSnapshotProducer() throws Exception {
        System.out.println("\n----- PREPARE PORTAL FOR TASK 1 -----------------------------------------\n");

        System.out.println("\n----- RUN TASK 1 -----------------------------------------\n");

        var tmpFile2 = File.createTempFile("lzy", "test-result-2");
        var tmpFile3 = File.createTempFile("lzy", "test-result-3");
        var snapshotId = idGenerator.generate("snapshot-");

        try (var worker1 = startWorker()) {
            var taskOutputSlot = idGenerator.generate("/");

            String firstTaskId = startTask(
                "echo 'i-am-a-hacker' > $LZY_MOUNT/%s && echo 'hello'".formatted(taskOutputSlot),
                taskOutputSlot, worker1, true, snapshotId, stdlogsTopic);

            assertStdLogs(stdlogs, List.of(StdlogMessage.out(firstTaskId, "hello")), List.of());
            waitPortalCompleted();

            // task_1 clean up
            System.out.println("-- cleanup task1 scenario --");
            destroyChannel("channel_1");

            System.out.println("\n----- RUN TASK 2 -----------------------------------------\n");
            tmpFile2.deleteOnExit();

            var taskInputSlot2 = idGenerator.generate("/");

            String secondTaskId = startTask(
                "echo 'x' && $LZY_MOUNT/sbin/cat $LZY_MOUNT/%s > %s"
                    .formatted(taskInputSlot2, tmpFile2.getAbsolutePath()),
                taskInputSlot2, worker1, false, snapshotId, stdlogsTopic);

            assertStdLogs(stdlogs, List.of(StdlogMessage.out(secondTaskId, "x")), List.of());
            waitPortalCompleted();

            System.out.println("\n----- RUN TASK 3 -----------------------------------------\n");


            tmpFile3.deleteOnExit();

            var taskInputSlot3 = idGenerator.generate("/");

            String thirdTaskId = startTask(
                "echo 'x' && $LZY_MOUNT/sbin/cat $LZY_MOUNT/%s > %s"
                    .formatted(taskInputSlot3, tmpFile3.getAbsolutePath()),
                taskInputSlot3, worker1, false, snapshotId, stdlogsTopic);

            assertStdLogs(stdlogs, List.of(StdlogMessage.out(thirdTaskId, "x")), List.of());
            waitPortalCompleted();
        }

        finishStdlogsReader.finish();

        // task_3 clean up
        // task_2 clean up
        System.out.println("-- cleanup task_2 scenario --");
        destroyChannel("channel_2");

        System.out.println("-- cleanup task_3 scenario --");
        destroyChannel("channel_3");

        var result2 = new String(Files.readAllBytes(tmpFile2.toPath()));
        var result3 = new String(Files.readAllBytes(tmpFile3.toPath()));
        Assert.assertEquals("i-am-a-hacker\n", result2);
        Assert.assertEquals("i-am-a-hacker\n", result3);
    }

    @Test
    public void multipleConcurrentConsumerAndSingleSnapshotProducer() throws Exception {
        var snapshotId = idGenerator.generate("snapshot-");

        System.out.println("\n----- PREPARE PORTAL FOR TASK 1 -----------------------------------------\n");
        var tmpFile2 = File.createTempFile("lzy", "test-result-2");
        tmpFile2.deleteOnExit();

        var tmpFile3 = File.createTempFile("lzy", "test-result-3");
        tmpFile3.deleteOnExit();

        try (var worker1 = startWorker()) {
            System.out.println("\n----- RUN TASK 1 -----------------------------------------\n");

            var taskOutputSlot = idGenerator.generate("/");
            String firstTaskId = startTask(
                "echo 'i-am-a-hacker' > $LZY_MOUNT/%s && echo 'hello'".formatted(taskOutputSlot),
                taskOutputSlot, worker1, true, snapshotId, stdlogsTopic);

            assertStdLogs(stdlogs, List.of(StdlogMessage.out(firstTaskId, "hello")), List.of());
            waitPortalCompleted();

            // task_1 clean up
            System.out.println("-- cleanup task1 scenario --");
            destroyChannel("channel_1");

            System.out.println("\n----- RUN TASK 2 & TASK 3 -----------------------------------------\n");

            var taskInputSlot2 = idGenerator.generate("/");
            var taskInputSlot3 = idGenerator.generate("/");

            startTask(
                "$LZY_MOUNT/sbin/cat $LZY_MOUNT/%s > %s".formatted(taskInputSlot2, tmpFile2.getAbsolutePath()),
                taskInputSlot2, worker1, false, snapshotId, stdlogsTopic);
            startTask(
                "$LZY_MOUNT/sbin/cat $LZY_MOUNT/%s > %s".formatted(taskInputSlot3, tmpFile3.getAbsolutePath()),
                taskInputSlot3, worker1, false, snapshotId, stdlogsTopic);

            // wait
            waitPortalCompleted();
            finishStdlogsReader.finish();
        }

        // task_3 clean up
        // task_2 clean up
        System.out.println("-- cleanup task_2 scenario --");
        destroyChannel("channel_2");

        System.out.println("-- cleanup task_3 scenario --");
        destroyChannel("channel_3");

        var result2 = new String(Files.readAllBytes(tmpFile2.toPath()));
        var result3 = new String(Files.readAllBytes(tmpFile3.toPath()));
        Assert.assertEquals("i-am-a-hacker\n", result2);
        Assert.assertEquals("i-am-a-hacker\n", result3);
    }
}
