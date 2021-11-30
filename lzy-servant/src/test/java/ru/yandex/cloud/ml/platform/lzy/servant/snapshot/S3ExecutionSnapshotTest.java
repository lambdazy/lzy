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

public class S3ExecutionSnapshotTest {
    private final String BUCKET_NAME = "bucket-test";
    private final String ACCESS_KEY = "access-key";
    private final String SECRET_KEY = "secret-key";
    private final String REGION = "us-west-2";
    private final String SERVICE_ENDPOINT = "http://localhost:8001";
    private static final String PATH_STYLE_ACCESS_ENABLED = "true";

    private final S3Mock api = new S3Mock.Builder().withPort(8001).withInMemoryBackend().build();
    private final AmazonS3 client = AmazonS3ClientBuilder
            .standard()
            .withPathStyleAccessEnabled(true)
            .withEndpointConfiguration(new EndpointConfiguration(SERVICE_ENDPOINT, REGION))
            .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
            .build();

    @Before
    public void setUp() {
        api.start();
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
        return "/task/" + taskId + "/slot/" + slotName;
    }
    private String getObjectContent(String key) throws IOException {
        return IOUtils.toString(
                client.getObject(new GetObjectRequest(BUCKET_NAME, key))
                      .getObjectContent(),
                StandardCharsets.UTF_8
        );
    }

    @Test
    public void testMultipleSnapshots() throws IOException {
        try (MockedStatic<Environment> environment = Mockito.mockStatic(Environment.class)) {
            environment.when(Environment::getBucketName).thenReturn(BUCKET_NAME);
            environment.when(Environment::getAccessKey).thenReturn(ACCESS_KEY);
            environment.when(Environment::getSecretKey).thenReturn(SECRET_KEY);
            environment.when(Environment::getRegion).thenReturn(REGION);
            environment.when(Environment::getServiceEndpoint).thenReturn(SERVICE_ENDPOINT);
            environment.when(Environment::getPathStyleAccessEnabled).thenReturn(PATH_STYLE_ACCESS_ENABLED);

            var firstSnapshot = new S3ExecutionSnapshot("first-task-id");
            var secondSnapshot = new S3ExecutionSnapshot("second-task-id");
            var thirdSnapshot = new S3ExecutionSnapshot("third-task-id");

            var firstSlot = slotForName("first");
            var secondSlot = slotForName("second");
            var thirdSlot = slotForName("third");
            var fourthSlot = slotForName("fourth");
            var fifthSlot = slotForName("fifth");

            firstSnapshot.onChunkInput(ByteString.copyFrom("Hello ", "utf-8"), firstSlot);
            secondSnapshot.onChunkInput(ByteString.copyFrom("Moni ", "utf-8"), fourthSlot);
            firstSnapshot.onChunkInput(ByteString.copyFrom("Bonjour ", "utf-8"), secondSlot);
            firstSnapshot.onChunkInput(ByteString.copyFrom("le ", "utf-8"), secondSlot);
            secondSnapshot.onChunkInput(ByteString.copyFrom("Dziko ", "utf-8"), fourthSlot);
            firstSnapshot.onChunkInput(ByteString.copyFrom("world!", "utf-8"), firstSlot);
            thirdSnapshot.onChunkInput(ByteString.copyFrom("Ciao ", "utf-8"), fifthSlot);
            secondSnapshot.onChunkInput(ByteString.copyFrom("Hola ", "utf-8"), thirdSlot);
            firstSnapshot.onChunkInput(ByteString.copyFrom("monde", "utf-8"), secondSlot);
            secondSnapshot.onChunkInput(ByteString.copyFrom("mundo!", "utf-8"), thirdSlot);
            thirdSnapshot.onChunkInput(ByteString.copyFrom("mondo!", "utf-8"), fifthSlot);
            secondSnapshot.onChunkInput(ByteString.copyFrom("Lapansi!", "utf-8"), fourthSlot);

            firstSnapshot.onFinish(firstSlot);
            firstSnapshot.onFinish(secondSlot);
            secondSnapshot.onFinish(thirdSlot);
            secondSnapshot.onFinish(fourthSlot);
            thirdSnapshot.onFinish(fifthSlot);

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
}
