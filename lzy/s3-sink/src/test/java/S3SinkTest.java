import ai.lzy.iam.test.BaseTestWithIam;
import ai.lzy.kafka.Job;
import ai.lzy.kafka.JobExecutor;
import ai.lzy.kafka.Main;
import ai.lzy.kafka.ServiceConfig;
import ai.lzy.model.db.test.DatabaseTestUtils;
import ai.lzy.util.kafka.KafkaHelper;
import ai.lzy.v1.common.LMST;
import ai.lzy.v1.kafka.KafkaS3Sink;
import ai.lzy.v1.kafka.S3SinkServiceGrpc;
import com.adobe.testing.s3mock.junit4.S3MockRule;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3URI;
import io.github.embeddedkafka.EmbeddedK;
import io.github.embeddedkafka.EmbeddedKafka;
import io.github.embeddedkafka.EmbeddedKafkaConfig$;
import io.grpc.ManagedChannel;
import io.micronaut.context.ApplicationContext;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.*;
import scala.collection.immutable.Map$;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;

public class S3SinkTest{
    private static ApplicationContext context;
    private static Main app;
    private static BaseTestWithIam iamContext;
    private static EmbeddedK kafka;
    private static ManagedChannel channel;
    private static S3SinkServiceGrpc.S3SinkServiceBlockingStub stub;

    @ClassRule
    public static PreparedDbRule iamDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});
    @ClassRule
    public static S3MockRule s3MockRule = S3MockRule.builder()
        .withHttpPort(12345)
        .build();
    private static KafkaProducer<String, byte[]> producer;
    private static AmazonS3 s3Client;
    private static JobExecutor executor;

    @BeforeClass
    public static void setUp() throws Exception {
        iamContext = new BaseTestWithIam();
        iamContext.setUp(DatabaseTestUtils.preparePostgresConfig("iam", iamDb.getConnectionInfo()));

        scala.collection.immutable.Map<String, String> conf = Map$.MODULE$.empty();
        var config = EmbeddedKafkaConfig$.MODULE$.apply(
            8001,
            8002,
            conf,
            conf,
            conf
        );

        kafka = EmbeddedKafka.start(config);

        KafkaHelper.USE_AUTH.set(false);

        Map<String, Object> appConf = Map.of(
            "s3-sink.kafka.enabled", "true",
            "s3-sink.kafka.bootstrapServers", "localhost:8001",
            "s3-sink.iam.address", "localhost:" + iamContext.getPort()
        );

        context = ApplicationContext.run(appConf);
        app = context.getBean(Main.class);

        var serviceConfig = context.getBean(ServiceConfig.class);
        channel = newGrpcChannel(serviceConfig.getAddress(), S3SinkServiceGrpc.SERVICE_NAME);


        var creds = iamContext.getClientConfig().createRenewableToken();

        stub = newBlockingClient(S3SinkServiceGrpc.newBlockingStub(channel), "Test",
            () -> creds.get().token());

        var helper = context.getBean(KafkaHelper.class);

        producer = new KafkaProducer<>(helper.toProperties());
        s3Client = AmazonS3ClientBuilder.standard()
            .withPathStyleAccessEnabled(true)
            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:12345", "us-west-1"))
            .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
            .build();

        executor = context.getBean(JobExecutor.class);
    }

    @Test
    public void testSimple() throws Exception {
        var topic = "testSimpleTopic";
        s3Client.createBucket("testsimple");

        writeIntoKafka(topic, "Some simple data");
        var resp = stub.start(KafkaS3Sink.StartRequest.newBuilder()
                .setStorageConfig(LMST.StorageConfig.newBuilder()
                    .setS3(LMST.S3Credentials.newBuilder()
                        .setEndpoint("localhost:12345")
                        .setAccessToken("test")
                        .setSecretToken("test")
                        .build())
                    .setUri("s3://testsimple/a")
                    .build())
                .setTopicName(topic)
            .build());

        var fut = executor.setupWaiter(resp.getJobId());

        stub.stop(KafkaS3Sink.StopRequest.newBuilder()
            .setJobId(resp.getJobId())
            .build());

        fut.get();

        Assert.assertEquals("Some simple data", readFromS3("s3://testsimple/a"));
    }

    @Test
    public void testWriteAfterStart() throws Exception {
        var topic = "testWriteAfterStart";
        s3Client.createBucket("testwriteafterstart");

        var uri = "s3://testwriteafterstart/a";

        var resp = stub.start(KafkaS3Sink.StartRequest.newBuilder()
            .setStorageConfig(LMST.StorageConfig.newBuilder()
                .setS3(LMST.S3Credentials.newBuilder()
                    .setEndpoint("localhost:12345")
                    .setAccessToken("test")
                    .setSecretToken("test")
                    .build())
                .setUri(uri)
                .build())
            .setTopicName(topic)
            .build());

        var fut = executor.setupWaiter(resp.getJobId());

        writeIntoKafka(topic, "1\n");
        writeIntoKafka(topic, "2\n");
        writeIntoKafka(topic, "3\n");

        stub.stop(KafkaS3Sink.StopRequest.newBuilder()
            .setJobId(resp.getJobId())
            .build());

        fut.get();

        Assert.assertEquals("1\n2\n3\n", readFromS3(uri));
    }

    @Test
    public void testLargeData() throws Exception {
        var topic = "testLargeData";
        s3Client.createBucket("testlargedata");

        var uri = "s3://testlargedata/a";

        var resp = stub.start(KafkaS3Sink.StartRequest.newBuilder()
            .setStorageConfig(LMST.StorageConfig.newBuilder()
                .setS3(LMST.S3Credentials.newBuilder()
                    .setEndpoint("localhost:12345")
                    .setAccessToken("test")
                    .setSecretToken("test")
                    .build())
                .setUri(uri)
                .build())
            .setTopicName(topic)
            .build());

        var fut = executor.setupWaiter(resp.getJobId());

        char[] chars = new char[1024 * 1023];  // 1023 Kb of data, max size of kafka message
        Arrays.fill(chars, 'a');

        String largeString = new String(chars);

        for (int i = 0; i < 6; i++) {  // Writing 6 time to fill job's buffer
            writeIntoKafka(topic, largeString);
        }

        stub.stop(KafkaS3Sink.StopRequest.newBuilder()
            .setJobId(resp.getJobId())
            .build());

        fut.get();

        var res = readFromS3(uri);

        Assert.assertTrue(  // Do not check content to not print difference in logs if not equals
            (largeString + largeString + largeString + largeString + largeString + largeString).equals(res)
        );
    }

    @Test
    public void testParallel() throws Exception {
        var topic = "testParallel";

        var jobCount = 100;  // Testing of parallel 100 jobs

        List<CompletableFuture<Job.PollResult>> futures = new ArrayList<>();
        List<String> ids = new ArrayList<>();

        s3Client.createBucket("testparallel");

        for (int i = 0; i < jobCount; i ++) {

            var resp = stub.start(KafkaS3Sink.StartRequest.newBuilder()
                .setStorageConfig(LMST.StorageConfig.newBuilder()
                    .setS3(LMST.S3Credentials.newBuilder()
                        .setEndpoint("localhost:12345")
                        .setAccessToken("test")
                        .setSecretToken("test")
                        .build())
                    .setUri("s3://testparallel/" + i)
                    .build())
                .setTopicName(topic + i)
                .build());

            ids.add(resp.getJobId());

            var fut = executor.setupWaiter(resp.getJobId());
            futures.add(fut);
        }

        var msgFutures = new ArrayList<Future<RecordMetadata>>(10 * jobCount);

        for (int j = 0; j < 10; j ++) {  // Generating 10 messages for every job
            for (int i = 0; i < jobCount; i ++) {
                msgFutures.add(writeIntoKafkaAsync(topic + i, "Some simple data"));
            }
        }

        for (var fut: msgFutures) {
            fut.get();
        }


        for (int i = 0; i < jobCount; i ++) {  // Completing all jobs
            stub.stop(KafkaS3Sink.StopRequest.newBuilder()
                .setJobId(ids.get(i))
                .build());
        }

        for (int i = 0; i < jobCount; i ++) {  // Waiting for all jobs to stop
            futures.get(i).get();
        }

        var data = new String(new char[10]).replace("\0", "Some simple data");

        for (int i = 0; i < jobCount; i ++) {  // Asserting all data
            Assert.assertEquals(data, readFromS3("s3://testparallel/" + i));
        }
    }



    @AfterClass
    public static void tearDown() throws Exception {
        iamContext.after();
        kafka.stop(true);

        app.close();
        context.stop();

        channel.shutdownNow();
        channel.awaitTermination(100, TimeUnit.MILLISECONDS);

        producer.close();
    }

    public void writeIntoKafka(String topic, String data) throws ExecutionException, InterruptedException {
        producer.send(new ProducerRecord<>(topic, data.getBytes())).get();
    }
    public Future<RecordMetadata> writeIntoKafkaAsync(String topic, String data) {
        return producer.send(new ProducerRecord<>(topic, data.getBytes()));
    }

    public String readFromS3(String uri) throws IOException {
        var url = new AmazonS3URI(uri);

        var obj = s3Client.getObject(url.getBucket(), url.getKey());

        return new String(obj.getObjectContent().readAllBytes(), StandardCharsets.UTF_8);
    }
}
