package ai.lzy.channelmanager;

import ai.lzy.channelmanager.config.ChannelManagerConfig;
import ai.lzy.channelmanager.test.InjectedFailures;
import ai.lzy.model.utils.FreePortFinder;
import ai.lzy.slots.SlotsExecutionContext;
import ai.lzy.slots.SlotsService;
import ai.lzy.util.grpc.GrpcUtils;
import ai.lzy.v1.channel.LCMPS;
import ai.lzy.v1.channel.LzyChannelManagerGrpc;
import ai.lzy.v1.common.LC;
import ai.lzy.v1.common.LMS;
import ai.lzy.v1.common.LMST;
import com.adobe.testing.s3mock.junit4.S3MockRule;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3URI;
import com.google.common.net.HostAndPort;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.apache.commons.io.FileUtils;
import org.junit.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static ai.lzy.util.grpc.GrpcUtils.NO_AUTH;
import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcServer;

public class SlotsIntegrationTest extends IamOnlyChannelManagerContextTests {
    private static final int s3MockPort = FreePortFinder.find(1000, 2000);
    private static final int serverPort = FreePortFinder.find(2000, 3000);

    private static final String SLOTS_ADDRESS = "localhost:" + serverPort;
    private static final String S3_ADDRESS = "http://localhost:" + s3MockPort;
    private static final String FS_ROOT = "/tmp/lzy_channel_manager_test";

    @ClassRule
    public static S3MockRule s3MockRule = S3MockRule.builder()
        .withHttpPort(s3MockPort)
        .silent()
        .build();
    private static AmazonS3 s3Client;

    private static String workflowName;
    private static ApiTest.User user;
    private static LzyChannelManagerGrpc.LzyChannelManagerBlockingStub publicClient;
    private static Server mockedLzyServiceServer;
    private static Server server;
    private static SlotsService slotsService;

    @BeforeClass
    public static void before() throws Exception {
        setUp();

        GrpcUtils.setIsRetriesEnabled(false);

        var config = context.getBean(ChannelManagerConfig.class);

        workflowName = "wfName";
        try (final var iamClient = new ApiTest.IamClient(config.getIam())) {
            user = iamClient.createUser("workflowUser");
            iamClient.addWorkflowAccess(user, workflowName);
        }

        publicClient = newBlockingClient(LzyChannelManagerGrpc.newBlockingStub(channelManagerGrpcChannel),
            "AuthPublicTest", () -> user.credentials().token());

        var mockedLzyServiceAddress = HostAndPort.fromString(config.getLzyServiceAddress());
        mockedLzyServiceServer = newGrpcServer(mockedLzyServiceAddress, NO_AUTH)
            .addService(new LzyServiceMock())
            .build();
        mockedLzyServiceServer.start();

        Files.createDirectories(Path.of(FS_ROOT));

        s3Client = AmazonS3ClientBuilder.standard()
            .withPathStyleAccessEnabled(true)
            .withEndpointConfiguration(
                new AwsClientBuilder.EndpointConfiguration("http://localhost:" + s3MockPort, "us-west-1"))
            .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
            .build();

        slotsService = new SlotsService();

        server = ServerBuilder
            .forPort(serverPort)
            .addService(slotsService)
            .build();
        server.start();
    }

    @AfterClass
    public static void afterClass() throws InterruptedException, IOException {
        InjectedFailures.assertClean();

        context.close();
        mockedLzyServiceServer.shutdownNow();
        mockedLzyServiceServer.awaitTermination();

        FileUtils.deleteDirectory(Path.of(FS_ROOT).toFile());
        server.shutdownNow();
        server.awaitTermination();

        tearDown();
    }

    @After
    public void after() {
        privateClient.destroyAll(LCMPS.DestroyAllRequest.newBuilder()
            .setExecutionId("execId")
            .build());
    }

    @Test
    public void testStorageToConsumer() throws Exception {
        s3Client.createBucket("test-storage-to-consumer");
        writeToS3("s3://test-storage-to-consumer/test", "test");

        var chan = privateClient.getOrCreate(LCMPS.GetOrCreateRequest.newBuilder()
            .setExecutionId("execId")
            .setWorkflowName(workflowName)
            .setUserId(user.id())
            .setProducer(LC.PeerDescription.StoragePeer.newBuilder()
                .setStorageUri("s3://test-storage-to-consumer/test")
                .setS3(LMST.S3Credentials.newBuilder()
                    .setEndpoint(S3_ADDRESS)
                    .build())
                .build())
            .build());

        var channelId = chan.getChannelId();

        var slots = List.of(
            LMS.Slot.newBuilder()
                .setDirection(LMS.Slot.Direction.INPUT)
                .setName("storage-to-consumer/in")
                .build()
        );

        var ctx = new SlotsExecutionContext(
            Path.of(FS_ROOT),
            slots,
            Map.of("storage-to-consumer/in", channelId),
            publicClient,
            "rid",
            "execId",
            "tid",
            SLOTS_ADDRESS,
            () -> user.credentials().token(),
            slotsService
        );

        ctx.beforeExecution();

        Assert.assertEquals("test", Files.readString(Path.of(FS_ROOT, "/storage-to-consumer/in")));
        ctx.afterExecution();
        ctx.close();
    }

    @Test
    public void testProducerToStorage() throws Exception {
        s3Client.createBucket("test-producer-to-storage");

        var chan = privateClient.getOrCreate(LCMPS.GetOrCreateRequest.newBuilder()
            .setExecutionId("execId")
            .setWorkflowName(workflowName)
            .setUserId(user.id())
            .setConsumer(LC.PeerDescription.StoragePeer.newBuilder()
                .setStorageUri("s3://test-producer-to-storage/test")
                .setS3(LMST.S3Credentials.newBuilder()
                    .setEndpoint(S3_ADDRESS)
                    .build())
                .build())
            .build());

        var channelId = chan.getChannelId();

        var slots = List.of(
            LMS.Slot.newBuilder()
                .setDirection(LMS.Slot.Direction.OUTPUT)
                .setName("producer-to-storage/out")
                .build()
        );

        var ctx = new SlotsExecutionContext(
            Path.of(FS_ROOT),
            slots,
            Map.of("producer-to-storage/out", channelId),
            publicClient,
            "rid",
            "execId",
            "tid",
            SLOTS_ADDRESS,
            () -> user.credentials().token(),
            slotsService
        );

        ctx.beforeExecution();
        Files.writeString(Path.of(FS_ROOT, "/producer-to-storage/out"), "test");

        ctx.afterExecution();

        Assert.assertEquals("test", readFromS3("s3://test-producer-to-storage/test"));
        ctx.close();
    }

    @Test
    public void testMultiConsumer() throws Exception {
        s3Client.createBucket("test-multi-consumer");

        var chan = privateClient.getOrCreate(LCMPS.GetOrCreateRequest.newBuilder()
            .setExecutionId("execId")
            .setWorkflowName(workflowName)
            .setUserId(user.id())
            .setConsumer(LC.PeerDescription.StoragePeer.newBuilder()
                .setStorageUri("s3://test-multi-consumer/test")
                .setS3(LMST.S3Credentials.newBuilder()
                    .setEndpoint(S3_ADDRESS)
                    .build())
                .build())
            .build());

        var channelId = chan.getChannelId();

        var slots = List.of(
            LMS.Slot.newBuilder()
                .setDirection(LMS.Slot.Direction.OUTPUT)
                .setName("multi-consumer/out")
                .build()
        );

        var ctx = new SlotsExecutionContext(
            Path.of(FS_ROOT),
            slots,
            Map.of("multi-consumer/out", channelId),
            publicClient,
            "rid",
            "execId",
            "tid",
            SLOTS_ADDRESS,
            () -> user.credentials().token(),
            slotsService
        );

        var slots1 = List.of(
            LMS.Slot.newBuilder()
                .setDirection(LMS.Slot.Direction.INPUT)
                .setName("multi-consumer/in1")
                .build(),
            LMS.Slot.newBuilder()
                .setDirection(LMS.Slot.Direction.INPUT)
                .setName("multi-consumer/in2")
                .build(),
            LMS.Slot.newBuilder()
                .setDirection(LMS.Slot.Direction.INPUT)
                .setName("multi-consumer/in3")
                .build()
        );

        var ctx2 = new SlotsExecutionContext(
            Path.of(FS_ROOT),
            slots1,
            Map.of(
                "multi-consumer/in1", channelId,
                "multi-consumer/in2", channelId,
                "multi-consumer/in3", channelId
            ),
            publicClient,
            "rid",
            "execId",
            "tid",
            SLOTS_ADDRESS,
            () -> user.credentials().token(),
            slotsService
        );

        ctx.beforeExecution();
        Files.writeString(Path.of(FS_ROOT, "/multi-consumer/out"), "test");

        ctx.afterExecution();

        Assert.assertEquals("test", readFromS3("s3://test-multi-consumer/test"));

        ctx2.beforeExecution();
        Assert.assertEquals("test", Files.readString(Path.of(FS_ROOT, "/multi-consumer/in1")));
        Assert.assertEquals("test", Files.readString(Path.of(FS_ROOT, "/multi-consumer/in2")));
        Assert.assertEquals("test", Files.readString(Path.of(FS_ROOT, "/multi-consumer/in3")));

        ctx2.afterExecution();
        ctx.close();
        ctx2.close();
    }

    public String readFromS3(String uri) throws IOException {
        var url = new AmazonS3URI(uri);

        var obj = s3Client.getObject(url.getBucket(), url.getKey());

        return new String(obj.getObjectContent().readAllBytes(), StandardCharsets.UTF_8);
    }

    public void writeToS3(String uri, String data) {
        var url = new AmazonS3URI(uri);

        s3Client.putObject(url.getBucket(), url.getKey(), data);
    }

}
