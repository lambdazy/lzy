package ru.yandex.cloud.ml.platform.lzy.servant.snapshot;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.google.protobuf.ByteString;
import org.apache.commons.io.IOUtils;
import org.junit.*;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.data.DataSchema;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import io.findify.s3mock.S3Mock;
import ru.yandex.qe.s3.util.Environment;

public class S3SlotSnapshotTest {
    private final String SERVICE_ENDPOINT = "http://localhost:8001";

    private final S3Mock api = new S3Mock.Builder().withPort(8001).withInMemoryBackend().build();
    private final MockedStatic<Environment> environment = Mockito.mockStatic(Environment.class);
    private final AmazonS3 client = AmazonS3ClientBuilder
            .standard()
            .withPathStyleAccessEnabled(true)
            .withEndpointConfiguration(new EndpointConfiguration(SERVICE_ENDPOINT, "us-west-2"))
            .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
            .build();

    @Before
    public void setUp() {
        api.start();

        environment.when(Environment::getBucketName).thenReturn("bucket-test");
        environment.when(Environment::getAccessKey).thenReturn("access-key");
        environment.when(Environment::getSecretKey).thenReturn("secret-key");
        environment.when(Environment::getRegion).thenReturn("us-west-2");
        environment.when(Environment::getServiceEndpoint).thenReturn(SERVICE_ENDPOINT);
        environment.when(Environment::getPathStyleAccessEnabled).thenReturn("true");
    }

    @After
    public void tearDown() {
        api.shutdown();
        environment.close();
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
        return "/task/" + taskId + "/slot/" + slotName;
    }

    private String getObjectContent(String key) throws IOException {
        return IOUtils.toString(
                client.getObject(new GetObjectRequest("bucket-test", key))
                        .getObjectContent(),
                StandardCharsets.UTF_8
        );
    }

    @Test
    public void testMultipleSnapshots() throws IOException {
        SlotSnapshotProvider snapshotProvider = new SlotSnapshotProvider.Cached(slot -> {
            if (slot.name().equals("first") || slot.name().equals("second")) {
                return new S3SlotSnapshot("first-task-id", slot);
            } else if (slot.name().equals("third") || slot.name().equals("fourth")) {
                return new S3SlotSnapshot("second-task-id", slot);
            } else if (slot.name().equals("fifth")) {
                return new S3SlotSnapshot("third-task-id", slot);
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
