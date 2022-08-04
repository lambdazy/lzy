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
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.google.protobuf.ByteString;
import io.findify.s3mock.S3Mock;
import org.apache.commons.io.IOUtils;
import org.junit.*;
import org.junit.runners.MethodSorters;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Stream;

@FixMethodOrder
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

    private static String generateKey(String slotName) {
        return "slot_" + slotName;
    }


    private String getObjectContent(String key) {
        try {
            return IOUtils.toString(
                    s3Client.getObject(new GetObjectRequest(BUCKET_NAME, key))
                            .getObjectContent(),
                    charset);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private URI generateURI(SlotInstance slotInstance) {
        return storage.getURI(BUCKET_NAME, generateKey(slotInstance.name()));
    }

    private static void awaitOpening(List<? extends LzySlotBase> slots) {
        slots.forEach(slot -> {
            //noinspection StatementWithEmptyBody,CheckStyle
            while (slot.state() == Operations.SlotStatus.State.UNBOUND
                    || slot.state() == Operations.SlotStatus.State.PREPARING) ;
        });
    }

    @Test
    public void testMultipleInputSlots() {
        List<String> names = List.of("first", "second", "third", "fourth", "fifth");
        List<S3SnapshotInputSlot> slots = names.stream()
                .map(S3SnapshotTest::slotForName)
                .map(slot -> new S3SnapshotInputSlot(slot, storage, BUCKET_NAME))
                .toList();
        List<String> messages = List.of("Hello world!", "Bonjour le monde!",
                "Hola mundo!", "Moni Dziko Lapansi", "Ciao mondo!");

        // store messages
        for (int i = 0; i < 5; i++) {
            LzyInputSlot slot = slots.get(i);
            slot.connect(generateURI(slot.instance()), Stream.of(ByteString.copyFrom(messages.get(i), charset)));
        }

        awaitOpening(slots);
        slots.forEach(LzySlot::destroy);

        // read and validate messages
        for (int i = 0; i < 5; i++) {
            String content = getObjectContent(generateKey(names.get(i)));
            Assert.assertEquals(content, messages.get(i));
        }
    }

    @Test
    public void testMultipleOutputSlots() {

    }
}
