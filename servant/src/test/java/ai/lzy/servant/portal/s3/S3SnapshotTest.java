package ai.lzy.servant.portal.s3;

import ai.lzy.fs.fs.LzyInputSlot;
import ai.lzy.fs.fs.LzySlot;
import ai.lzy.fs.slots.LzySlotBase;
import ai.lzy.fs.storage.AmazonStorageClient;
import ai.lzy.fs.storage.StorageClient;
import ai.lzy.model.Slot;
import ai.lzy.model.SlotInstance;
import ai.lzy.model.data.DataSchema;
import ai.lzy.v1.Operations;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.google.protobuf.ByteString;
import io.findify.s3mock.S3Mock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class S3SnapshotTest {
    private static final int S3_PORT = 8001;
    private static final String S3_ADDRESS = "http://localhost:" + S3_PORT;
    private static final String BUCKET_NAME = "lzy-snapshot-test-bucket";

    private final S3Mock s3 = new S3Mock.Builder().withPort(S3_PORT).withInMemoryBackend().build();
    private final StorageClient storage = new AmazonStorageClient("", "",
            URI.create(S3_ADDRESS), "transmitter", 10, 10);
    private final AmazonS3 s3Client = AmazonS3ClientBuilder
            .standard()
            .withPathStyleAccessEnabled(true)
            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(S3_ADDRESS, "us-west-2"))
            .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
            .build();

    private final Charset charset = StandardCharsets.UTF_8;

    @Before
    public void setUp() {
        s3.start();
        s3Client.createBucket(BUCKET_NAME);
    }

    @After
    public void tearDown() {
        s3.shutdown();
    }

    private static SlotInstance slotForName(String name) {
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
                public DataSchema contentType() {
                    return null;
                }
            }, "portalId", "channelId", new URI("scheme", "host", "/path", null));
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private String generateKey(SlotInstance slot) {
        return "slot_" + slot.name();
    }

    private URI generateURI(SlotInstance slot) {
        return storage.getURI(BUCKET_NAME, generateKey(slot));
    }

    private static void awaitOpening(List<? extends LzySlotBase> slots) {
        slots.forEach(slot -> {
            //noinspection StatementWithEmptyBody,CheckStyle
            while (slot.state() == Operations.SlotStatus.State.UNBOUND
                    || slot.state() == Operations.SlotStatus.State.PREPARING) ;
        });
    }

    @Test
    public void testMultipleSnapshotsStoreLoad() throws IOException {
        List<SlotInstance> instances = Stream.of("first", "second", "third", "fourth", "fifth")
                .map(S3SnapshotTest::slotForName).toList();
        List<String> messages = List.of("Hello world!", "Bonjour le monde!",
                "Hola mundo!", "Moni Dziko Lapansi", "Ciao mondo!");

        List<S3SnapshotInputSlot> inputSlots = instances.stream()
                .map(slot -> new S3SnapshotInputSlot(slot, generateKey(slot), BUCKET_NAME, storage))
                .toList();
        // store messages
        for (int i = 0; i < 5; i++) {
            LzyInputSlot slot = inputSlots.get(i);
            slot.connect(generateURI(slot.instance()), Stream.of(ByteString.copyFrom(messages.get(i), charset)));
        }

        awaitOpening(inputSlots);
        inputSlots.forEach(LzySlot::destroy);

        List<S3SnapshotOutputSlot> outputSlots = instances.stream()
                .map(slot -> new S3SnapshotOutputSlot(slot, generateKey(slot), BUCKET_NAME, storage))
                .toList();
        // read and validate previously stored data
        for (int i = 0; i < 5; i++) {
            String content = outputSlots.get(i).readFromPosition(0)
                    .map(byteString -> byteString.toString(charset))
                    .collect(Collectors.joining());
            Assert.assertEquals(content, messages.get(i));
        }
    }
}
