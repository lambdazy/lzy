package ru.yandex.cloud.ml.platform.lzy.servant.snapshot;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.google.protobuf.ByteString;
import io.findify.s3mock.S3Mock;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.data.DataSchema;
import ru.yandex.cloud.ml.platform.lzy.servant.storage.AmazonStorageClient;
import ru.yandex.cloud.ml.platform.lzy.servant.storage.StorageClient;

public class S3SlotSnapshotTest {
    private static final String SERVICE_ENDPOINT = "http://localhost:8001";
    private static final String BUCKET = "lzy-bucket";

    private final S3Mock api = new S3Mock.Builder().withPort(8001).withInMemoryBackend().build();
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
        api.start();
        client.createBucket(BUCKET);
    }

    @After
    public void tearDown() {
        api.shutdown();
    }

    private Slot slotForName(String name) {
        return new Slot() {
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
            public DataSchema contentType() {
                return null;
            }
        };
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
        SlotSnapshotProvider snapshotProvider = new SlotSnapshotProvider.Cached(slot -> {
            if (slot.name().equals("first") || slot.name().equals("second")) {
                return new S3SlotSnapshot("first-task-id", BUCKET, slot, storage);
            } else if (slot.name().equals("third") || slot.name().equals("fourth")) {
                return new S3SlotSnapshot("second-task-id", BUCKET, slot, storage);
            } else if (slot.name().equals("fifth")) {
                return new S3SlotSnapshot("third-task-id", BUCKET, slot, storage);
            } else {
                throw new RuntimeException("Unknown slot: " + slot.name());
            }
        });

        var firstSlot = slotForName("first");
        var secondSlot = slotForName("second");
        var thirdSlot = slotForName("third");
        var fourthSlot = slotForName("fourth");
        var fifthSlot = slotForName("fifth");

        snapshotProvider.slotSnapshot(firstSlot).onChunk(ByteString.copyFrom("Hello ", "utf-8"));
        snapshotProvider.slotSnapshot(fourthSlot).onChunk(ByteString.copyFrom("Moni ", "utf-8"));
        snapshotProvider.slotSnapshot(secondSlot).onChunk(ByteString.copyFrom("Bonjour ", "utf-8"));
        snapshotProvider.slotSnapshot(secondSlot).onChunk(ByteString.copyFrom("le ", "utf-8"));
        snapshotProvider.slotSnapshot(fourthSlot).onChunk(ByteString.copyFrom("Dziko ", "utf-8"));
        snapshotProvider.slotSnapshot(firstSlot).onChunk(ByteString.copyFrom("world!", "utf-8"));
        snapshotProvider.slotSnapshot(fifthSlot).onChunk(ByteString.copyFrom("Ciao ", "utf-8"));
        snapshotProvider.slotSnapshot(thirdSlot).onChunk(ByteString.copyFrom("Hola ", "utf-8"));
        snapshotProvider.slotSnapshot(secondSlot).onChunk(ByteString.copyFrom("monde", "utf-8"));
        snapshotProvider.slotSnapshot(thirdSlot).onChunk(ByteString.copyFrom("mundo!", "utf-8"));
        snapshotProvider.slotSnapshot(fifthSlot).onChunk(ByteString.copyFrom("mondo!", "utf-8"));
        snapshotProvider.slotSnapshot(fourthSlot).onChunk(ByteString.copyFrom("Lapansi!", "utf-8"));

        snapshotProvider.slotSnapshot(firstSlot).onFinish();
        snapshotProvider.slotSnapshot(secondSlot).onFinish();
        snapshotProvider.slotSnapshot(thirdSlot).onFinish();
        snapshotProvider.slotSnapshot(fourthSlot).onFinish();
        snapshotProvider.slotSnapshot(fifthSlot).onFinish();

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
}
