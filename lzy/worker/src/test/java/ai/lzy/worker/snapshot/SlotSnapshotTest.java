package ai.lzy.worker.snapshot;

import ai.lzy.fs.snapshot.SlotSnapshot;
import ai.lzy.fs.snapshot.SlotSnapshotImpl;
import ai.lzy.fs.storage.AmazonStorageClient;
import ai.lzy.fs.storage.StorageClient;
import ai.lzy.model.DataScheme;
import ai.lzy.model.slot.Slot;
import ai.lzy.model.slot.SlotInstance;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.google.protobuf.ByteString;
import io.findify.s3mock.S3Mock;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

public class SlotSnapshotTest {
    private static final int S3_PORT = 8001;
    private static final String SERVICE_ENDPOINT = "http://localhost:" + S3_PORT;
    private static final String BUCKET = "lzy-bucket";

    private final S3Mock s3Mock = new S3Mock.Builder().withPort(S3_PORT).withInMemoryBackend().build();
    private final StorageClient storage =
        new AmazonStorageClient("", "", URI.create(SERVICE_ENDPOINT), "transmitter", 10, 10);
    private final AmazonS3 client = AmazonS3ClientBuilder
        .standard()
        .withPathStyleAccessEnabled(true)
        .withEndpointConfiguration(new EndpointConfiguration(SERVICE_ENDPOINT, "us-west-2"))
        .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
        .build();

    @Before
    public void setUp() {
        s3Mock.start();
        client.createBucket(BUCKET);
    }

    @After
    public void tearDown() {
        s3Mock.shutdown();
    }

    private SlotInstance slotForName(String name, String taskId) {
        try {
            return new SlotInstance(new Slot() {
                @Override
                public String name() {
                    return name;
                }

                @Override
                public Media media() {
                    return null;
                }

                @Override
                public Direction direction() {
                    return null;
                }

                @Override
                public DataScheme contentType() {
                    return null;
                }
            }, taskId, "channelId", new URI("scheme", "host", "/path", null));
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private String getKey(String taskId, String slotName) {
        return "task/" + taskId + "/slot/" + slotName;
    }

    private String getObjectContent(String key) throws IOException {
        return IOUtils.toString(
            client.getObject(new GetObjectRequest(BUCKET, key))
                .getObjectContent(),
            StandardCharsets.UTF_8
        );
    }

    @Test
    public void testMultipleSnapshots() throws IOException {

        final var firstSlot = slotForName("first", "first-task-id");
        final var secondSlot = slotForName("second", "first-task-id");
        final var thirdSlot = slotForName("third", "second-task-id");
        final var fourthSlot = slotForName("fourth", "second-task-id");
        final var fifthSlot = slotForName("fifth", "third-task-id");

        final SlotSnapshot first = new SlotSnapshotImpl(BUCKET, firstSlot, storage);
        final SlotSnapshot second = new SlotSnapshotImpl(BUCKET, secondSlot, storage);
        final SlotSnapshot third = new SlotSnapshotImpl(BUCKET, thirdSlot, storage);
        final SlotSnapshot fourth = new SlotSnapshotImpl(BUCKET, fourthSlot, storage);
        final SlotSnapshot fifth = new SlotSnapshotImpl(BUCKET, fifthSlot, storage);

        first.onChunk(ByteString.copyFrom("Hello ", "utf-8"));
        fourth.onChunk(ByteString.copyFrom("Moni ", "utf-8"));
        second.onChunk(ByteString.copyFrom("Bonjour ", "utf-8"));
        second.onChunk(ByteString.copyFrom("le ", "utf-8"));
        fourth.onChunk(ByteString.copyFrom("Dziko ", "utf-8"));
        first.onChunk(ByteString.copyFrom("world!", "utf-8"));
        fifth.onChunk(ByteString.copyFrom("Ciao ", "utf-8"));
        third.onChunk(ByteString.copyFrom("Hola ", "utf-8"));
        second.onChunk(ByteString.copyFrom("monde", "utf-8"));
        third.onChunk(ByteString.copyFrom("mundo!", "utf-8"));
        fifth.onChunk(ByteString.copyFrom("mondo!", "utf-8"));
        fourth.onChunk(ByteString.copyFrom("Lapansi!", "utf-8"));

        first.onFinish();
        second.onFinish();
        third.onFinish();
        fourth.onFinish();
        fifth.onFinish();

        Assert.assertEquals(
            getObjectContent(getKey("first-task-id", "first")),
            "Hello world!"
        );
        Assert.assertEquals(
            getObjectContent(getKey("first-task-id", "second")),
            "Bonjour le monde"
        );
        Assert.assertEquals(
            getObjectContent(getKey("second-task-id", "third")),
            "Hola mundo!"
        );
        Assert.assertEquals(
            getObjectContent(getKey("second-task-id", "fourth")),
            "Moni Dziko Lapansi!"
        );
        Assert.assertEquals(
            getObjectContent(getKey("third-task-id", "fifth")),
            "Ciao mondo!"
        );
    }

    @Test
    public void testWriteAndFinishFromVariousThreads() throws InterruptedException {
        for (int i = 0; i < 10; i++) {
            //Arrange
            final SlotInstance slot = slotForName("test-slot", "test-slot-task-id");
            final SlotSnapshot slotSnapshot = new SlotSnapshotImpl(BUCKET, slot, storage);
            final CountDownLatch writeLatch = new CountDownLatch(1);
            final CountDownLatch finishLatch = new CountDownLatch(1);
            final AtomicReference<Exception> thrown = new AtomicReference<>(null);

            //Act
            new Thread(() -> {
                slotSnapshot.onChunk(ByteString.copyFrom("str", StandardCharsets.UTF_8));
                writeLatch.countDown();
            }).start();
            writeLatch.await();
            new Thread(() -> {
                try {
                    slotSnapshot.onFinish();
                } catch (Exception e) {
                    thrown.set(e);
                } finally {
                    finishLatch.countDown();
                }
            }).start();
            finishLatch.await();

            //Assert
            Assert.assertNull(thrown.get());
        }
    }
}