import ai.lzy.iam.test.BaseTestWithIam;
import ai.lzy.kafka.s3sink.Job;
import ai.lzy.kafka.s3sink.JobExecutor;
import ai.lzy.kafka.s3sink.Main;
import ai.lzy.kafka.s3sink.S3SinkMetrics;
import ai.lzy.kafka.s3sink.ServiceConfig;
import ai.lzy.longrunning.IdempotencyUtils;
import ai.lzy.longrunning.Operation;
import ai.lzy.model.db.test.DatabaseTestUtils;
import ai.lzy.util.grpc.GrpcUtils;
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
import jakarta.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.*;
import scala.collection.immutable.Map$;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;

public class S3SinkTest {
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
    private static S3SinkMetrics metrics;

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
            "s3-sink.kafka.bootstrap-servers", "localhost:8001",
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
            .withEndpointConfiguration(
                new AwsClientBuilder.EndpointConfiguration("http://localhost:12345", "us-west-1"))
            .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
            .build();

        executor = context.getBean(JobExecutor.class);
        metrics = context.getBean(S3SinkMetrics.class);
        Job.KAFKA_POLLING_TIMEOUT_MS.set(10);
        Job.UPLOADING_TIMEOUT_MS.set(10);
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

    @Before
    public void before() {
        metrics.activeSessions.clear();
        metrics.errors.clear();
        metrics.uploadedBytes.clear();
    }

    @After
    public void after() {
        Assert.assertEquals(0, (long) metrics.errors.get());
        Assert.assertEquals(0, (long) metrics.activeSessions.get());
    }

    @Test
    public void testIdempotent() throws Exception {
        var topic = "testIdempotentTopic";
        var taskId = UUID.randomUUID().toString();
        s3Client.createBucket("testidempotent");

        writeToKafka(null, taskId, topic, "Some simple data");
        var resp1 = GrpcUtils.withIdempotencyKey(stub, "idempotent").start(KafkaS3Sink.StartRequest.newBuilder()
            .setStorageConfig(LMST.StorageConfig.newBuilder()
                .setS3(LMST.S3Credentials.newBuilder()
                    .setEndpoint("http://localhost:12345")
                    .setAccessToken("test")
                    .setSecretToken("test")
                    .build())
                .setUri("s3://testidempotent/a")
                .build())
            .setTopicName(topic)
            .build());

        var resp2 = GrpcUtils.withIdempotencyKey(stub, "idempotent").start(KafkaS3Sink.StartRequest.newBuilder()
            .setStorageConfig(LMST.StorageConfig.newBuilder()
                .setS3(LMST.S3Credentials.newBuilder()
                    .setEndpoint("http://localhost:12345")
                    .setAccessToken("test")
                    .setSecretToken("test")
                    .build())
                .setUri("s3://testidempotent/a")
                .build())
            .setTopicName(topic)
            .build());

        Assert.assertEquals(resp1.getJobId(), resp2.getJobId());

        var fut = executor.setupWaiter(resp1.getJobId());
        executor.addActiveStream(resp1.getJobId(), taskId + "-stdout");

        stub.stop(KafkaS3Sink.StopRequest.newBuilder()
            .setJobId(resp1.getJobId())
            .build());

        fut.get();

        Assert.assertEquals("Some simple data", readFromS3("s3://testidempotent/a"));
    }

    @Test
    public void testSimple() throws Exception {
        var topic = "testSimpleTopic";
        var taskId = UUID.randomUUID().toString();
        s3Client.createBucket("testsimple");

        writeToKafka(null, taskId, topic, "Some simple data");
        var resp = stub.start(KafkaS3Sink.StartRequest.newBuilder()
                .setStorageConfig(LMST.StorageConfig.newBuilder()
                    .setS3(LMST.S3Credentials.newBuilder()
                        .setEndpoint("http://localhost:12345")
                        .setAccessToken("test")
                        .setSecretToken("test")
                        .build())
                    .setUri("s3://testsimple/a")
                    .build())
                .setTopicName(topic)
            .build());

        var fut = executor.setupWaiter(resp.getJobId());
        executor.addActiveStream(resp.getJobId(), taskId + "-stdout");

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
                    .setEndpoint("http://localhost:12345")
                    .setAccessToken("test")
                    .setSecretToken("test")
                    .build())
                .setUri(uri)
                .build())
            .setTopicName(topic)
            .build());

        var fut = executor.setupWaiter(resp.getJobId());

        writeToKafka(resp.getJobId(), resp.getJobId(), topic, "1\n2\n3\n");

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
                    .setEndpoint("http://localhost:12345")
                    .setAccessToken("test")
                    .setSecretToken("test")
                    .build())
                .setUri(uri)
                .build())
            .setTopicName(topic)
            .build());

        var fut = executor.setupWaiter(resp.getJobId());
        executor.addActiveStream(resp.getJobId(), resp.getJobId() + "-stdout");

        int messages = 10;
        int messageSize = 1024 * 1023; // 1023 Kb of data, max size of kafka message

        // Writing more than 6 time to fill job's buffer
        for (int i = 0; i < messages; i++) {
            var data = StringUtils.repeat((char) ('a' + i), messageSize);
            writeToKafka(null, resp.getJobId(), topic, data);
        }

        stub.stop(KafkaS3Sink.StopRequest.newBuilder()
            .setJobId(resp.getJobId())
            .build());

        fut.get();

        var res = readFromS3(uri);

        Assert.assertEquals(messageSize * messages, res.length());
        for (int i = 0; i < messages; ++i) {
            for (int j = 0; j < 5; ++j) {
                Assert.assertEquals((char) ('a' + i), res.charAt(i * messageSize + j));
            }
            for (int j = messageSize - 5; j < messageSize; ++j) {
                Assert.assertEquals((char) ('a' + i), res.charAt(i * messageSize + j));
            }
        }

        Assert.assertEquals(messageSize * messages, (long) metrics.uploadedBytes.get());
    }

    @Test
    public void testParallel() throws Exception {
        var topic = "testParallel";

        var jobCount = 100;  // Testing of parallel 100 jobs

        List<CompletableFuture<Job.PollResult>> futures = new ArrayList<>();
        List<String> ids = new ArrayList<>();

        s3Client.createBucket("testparallel");

        for (int i = 0; i < jobCount; i++) {
            var resp = stub.start(KafkaS3Sink.StartRequest.newBuilder()
                .setStorageConfig(LMST.StorageConfig.newBuilder()
                    .setS3(LMST.S3Credentials.newBuilder()
                        .setEndpoint("http://localhost:12345")
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

        for (int j = 0; j < 10; j++) {  // Generating 10 messages for every job
            for (int i = 0; i < jobCount; i++) {
                msgFutures.addAll(writeToKafkaAsync(ids.get(i), UUID.randomUUID().toString(), topic + i,
                    "Some simple data"));
            }
        }

        for (var fut: msgFutures) {
            fut.get();
        }


        for (int i = 0; i < jobCount; i++) {  // Completing all jobs
            stub.stop(KafkaS3Sink.StopRequest.newBuilder()
                .setJobId(ids.get(i))
                .build());
        }

        for (int i = 0; i < jobCount; i++) {  // Waiting for all jobs to stop
            futures.get(i).get();
        }

        var data = new String(new char[10]).replace("\0", "Some simple data");

        for (int i = 0; i < jobCount; i++) {  // Asserting all data
            Assert.assertEquals(data, readFromS3("s3://testparallel/" + i));
        }
    }

    public void writeToKafka(@Nullable String jobId, String taskId, String topic, String data)
        throws ExecutionException, InterruptedException {
        for (var f: writeToKafkaAsync(jobId, taskId, topic, data)) {
            f.get();
        }
    }

    public List<Future<RecordMetadata>> writeToKafkaAsync(@Nullable String jobId, String taskId,
                                                          String topic, String data) {
        var streamNameHeader = new RecordHeader("stream", "stdout".getBytes(StandardCharsets.UTF_8));

        if (jobId != null) {
            executor.addActiveStream(jobId, taskId + "-stdout");
        }

        var fut1 = producer.send(new ProducerRecord<>(topic, 0, taskId, data.getBytes(), List.of(streamNameHeader)));
        var fut2 = producer.send(new ProducerRecord<>(topic, 0, taskId, new byte[0],
            List.of(streamNameHeader, new RecordHeader("eos", new byte[0]))));
        return List.of(fut1, fut2);
    }

    public String readFromS3(String uri) throws IOException {
        var url = new AmazonS3URI(uri);

        var obj = s3Client.getObject(url.getBucket(), url.getKey());

        return new String(obj.getObjectContent().readAllBytes(), StandardCharsets.UTF_8);
    }
}
