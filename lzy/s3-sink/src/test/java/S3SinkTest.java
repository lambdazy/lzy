import ai.lzy.iam.test.BaseTestWithIam;
import ai.lzy.kafka.s3sink.Job;
import ai.lzy.kafka.s3sink.JobExecutor;
import ai.lzy.kafka.s3sink.Main;
import ai.lzy.kafka.s3sink.S3SinkMetrics;
import ai.lzy.kafka.s3sink.ServiceConfig;
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
import com.amazonaws.services.s3.model.S3ObjectSummary;
import io.github.embeddedkafka.EmbeddedK;
import io.github.embeddedkafka.EmbeddedKafka;
import io.github.embeddedkafka.EmbeddedKafkaConfig$;
import io.grpc.ManagedChannel;
import io.micronaut.context.ApplicationContext;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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

    private static final AtomicInteger nextTaskId = new AtomicInteger(1);

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
            "s3-sink.iam.address", "localhost:" + iamContext.getPort(),
            "s3-sink.complete-job-timeout", "5s",
            "s3-sink.upload-poll-interval", "50ms",
            "s3-sink.kafka-poll-interval", "50ms"
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
    public void idempotentStart() throws Exception {
        var topic = "idempotentStartTopic";
        var taskId = generateTaskId();
        s3Client.createBucket("idempotent-start");

        writeToKafka(taskId, topic, "Some simple data", /* eos */ true);

        var req = KafkaS3Sink.StartRequest.newBuilder()
            .setS3(LMST.S3Credentials.newBuilder()
                .setEndpoint("http://localhost:12345")
                .setAccessToken("test")
                .setSecretToken("test")
                .build())
            .setStoragePrefixUri("s3://idempotent-start/execution")
            .setTopicName(topic)
            .build();

        var resp1 = GrpcUtils.withIdempotencyKey(stub, "some-key").start(req);
        var resp2 = GrpcUtils.withIdempotencyKey(stub, "some-key").start(req);

        Assert.assertEquals(resp1.getJobId(), resp2.getJobId());

        var fut = executor.setupWaiter(resp1.getJobId());

        stub.stop(KafkaS3Sink.StopRequest.newBuilder()
            .setJobId(resp1.getJobId())
            .build());

        fut.get();

        var keys = listS3(req.getStoragePrefixUri());
        Assert.assertEquals(1, keys.size());

        Assert.assertEquals("Some simple data", readFromS3("s3://idempotent-start/" + keys.get(0)));
        Assert.assertEquals(16, (long) metrics.uploadedBytes.get());
    }

    @Test
    public void writeAfterStart() throws Exception {
        var topic = "writeAfterStart";
        var taskId = generateTaskId();
        s3Client.createBucket("write-after-start");

        var uri = "s3://write-after-start/execution";

        var resp = stub.start(KafkaS3Sink.StartRequest.newBuilder()
            .setS3(LMST.S3Credentials.newBuilder()
                .setEndpoint("http://localhost:12345")
                .setAccessToken("test")
                .setSecretToken("test")
                .build())
            .setStoragePrefixUri(uri)
            .setTopicName(topic)
            .build());

        var fut = executor.setupWaiter(resp.getJobId());

        writeToKafka(taskId, topic, "1\n2\n3\n", /* eos */ true);

        stub.stop(KafkaS3Sink.StopRequest.newBuilder()
            .setJobId(resp.getJobId())
            .build());

        fut.get();

        var keys = listS3(uri);
        Assert.assertEquals(1, keys.size());

        Assert.assertEquals("1\n2\n3\n", readFromS3("s3://write-after-start/" + keys.get(0)));
        Assert.assertEquals(6, (long) metrics.uploadedBytes.get());
    }

    @Test
    public void largeData() throws Exception {
        var topic = "largeData";
        var taskId = generateTaskId();
        s3Client.createBucket("large-data");

        var uri = "s3://large-data/execution";

        var resp = stub.start(KafkaS3Sink.StartRequest.newBuilder()
            .setS3(LMST.S3Credentials.newBuilder()
                .setEndpoint("http://localhost:12345")
                .setAccessToken("test")
                .setSecretToken("test")
                .build())
            .setStoragePrefixUri(uri)
            .setTopicName(topic)
            .build());

        var fut = executor.setupWaiter(resp.getJobId());

        int messages = 10;
        int messageSize = 1024 * 1023; // 1023 Kb of data, max size of kafka message

        // Write more than 6 times to fill job's buffer
        for (int i = 0; i < messages; i++) {
            var data = StringUtils.repeat((char) ('a' + i), messageSize);
            writeToKafka(taskId, topic, data, /* eos */ i + 1 == messages);
        }

        stub.stop(KafkaS3Sink.StopRequest.newBuilder()
            .setJobId(resp.getJobId())
            .build());

        fut.get();

        var keys = listS3(uri);
        Assert.assertEquals(1, keys.size());

        var res = readFromS3("s3://large-data/" + keys.get(0));

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
    public void parallel() throws Exception {
        var jobsCount = 100;  // Testing of parallel 100 jobs

        List<CompletableFuture<Job.JobStatus>> futures = new ArrayList<>();
        List<String> ids = new ArrayList<>();

        for (int i = 0; i < jobsCount; i++) {
            s3Client.createBucket("parallel-" + i);

            var resp = stub.start(KafkaS3Sink.StartRequest.newBuilder()
                .setS3(LMST.S3Credentials.newBuilder()
                    .setEndpoint("http://localhost:12345")
                    .setAccessToken("test")
                    .setSecretToken("test")
                    .build())
                .setStoragePrefixUri("s3://parallel-%d/execution".formatted(i))
                .setTopicName("parallel_" + i)
                .build());

            ids.add(resp.getJobId());

            var fut = executor.setupWaiter(resp.getJobId());
            futures.add(fut);
        }

        var msgFutures = new ArrayList<Future<RecordMetadata>>(10 * jobsCount);

        for (int j = 0; j < 10; j++) {  // Generating 10 messages for every job
            for (int i = 0; i < jobsCount; i++) {
                msgFutures.addAll(
                    writeToKafkaAsync(ids.get(i), "parallel_" + i, "Some simple data %d/%d".formatted(i, j), j == 9));
            }
        }

        for (var fut: msgFutures) {
            fut.get();
        }

        for (int i = 0; i < jobsCount; i++) {  // Completing all jobs
            stub.stop(KafkaS3Sink.StopRequest.newBuilder()
                .setJobId(ids.get(i))
                .build());
        }

        for (int i = 0; i < jobsCount; i++) {  // Waiting for all jobs to stop
            futures.get(i).get();
        }

        for (int i = 0; i < jobsCount; i++) {  // Asserting all data
            var keys = listS3("s3://parallel-%d/execution".formatted(i));
            Assert.assertEquals(1, keys.size());

            var expected = new StringBuilder();
            for (int j = 0; j < 10; ++j) {
                expected.append("Some simple data %d/%d".formatted(i, j));
            }
            Assert.assertEquals(expected.toString(), readFromS3("s3://parallel-%d/".formatted(i) + keys.get(0)));
        }
    }

    public void writeToKafka(String taskId, String topic, String data, boolean eos) throws Exception {
        for (var f : writeToKafkaAsync(taskId, topic, data, eos)) {
            f.get();
        }
    }

    public List<Future<RecordMetadata>> writeToKafkaAsync(String taskId, String topic, String data, boolean eos) {
        var streamNameHeader = new RecordHeader("stream", "out".getBytes(StandardCharsets.US_ASCII));

        var fut1 = producer.send(new ProducerRecord<>(topic, 0, taskId, data.getBytes(), List.of(streamNameHeader)));
        if (!eos) {
            return List.of(fut1);
        }

        var fut2 = producer.send(new ProducerRecord<>(topic, 0, taskId, new byte[0],
            List.of(streamNameHeader, new RecordHeader("eos", new byte[0]))));
        return List.of(fut1, fut2);
    }

    public List<String> listS3(String uri) {
        var url = new AmazonS3URI(uri);
        var objects = s3Client.listObjectsV2(url.getBucket());
        Assert.assertFalse(objects.isTruncated());
        return objects.getObjectSummaries().stream()
            .map(S3ObjectSummary::getKey)
            .toList();
    }

    public String readFromS3(String uri) throws IOException {
        var url = new AmazonS3URI(uri);
        var obj = s3Client.getObject(url.getBucket(), url.getKey());
        return new String(obj.getObjectContent().readAllBytes(), StandardCharsets.UTF_8);
    }

    private static String generateTaskId() {
        return "task-" + nextTaskId.getAndIncrement();
    }
}
