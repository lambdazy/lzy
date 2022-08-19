package ai.lzy.servant.portal;

import ai.lzy.model.GrpcConverter;
import ai.lzy.model.JsonUtils;
import ai.lzy.servant.agents.LzyAgentConfig;
import ai.lzy.servant.agents.LzyServant;
import ai.lzy.v1.LzyFsApi;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import io.findify.s3mock.S3Mock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.locks.LockSupport;

import static ai.lzy.test.GrpcUtils.*;

public abstract class PortalTest {

    protected ServerMock server;
    private ChannelManagerMock channelManager;
    private Map<String, LzyServant> servants;

    private static final int S3_PORT = 8001;
    protected static final String S3_ADDRESS = "http://localhost:" + S3_PORT;
    protected static final String BUCKET_NAME = "lzy-bucket";

    private S3Mock s3;

    @Before
    public void before() throws IOException {
        System.err.println("---> " + ForkJoinPool.commonPool().getParallelism());
        server = new ServerMock();
        server.start1();
        channelManager = new ChannelManagerMock();
        channelManager.start();
        servants = new HashMap<>();
    }

    @After
    public void after() throws InterruptedException, IOException {
        channelManager.stop();
        server.stop();
        for (var servant : servants.values()) {
            servant.close();
        }
        server = null;
        channelManager = null;
        servants = null;
    }

    @Test
    public void activatePortal() throws Exception {
        startServant("servant_1");

        server.waitServantStart("servant_1");
        server.assertPortalNotActive("servant_1");

        createChannel("servant_1:stdout");
        createChannel("servant_1:stderr");

        server.startPortalOn("servant_1");
        server.assertServantNotActive("servant_1");

        destroyChannel("servant_1:stdout");
        destroyChannel("servant_1:stderr");
    }

    protected void runWithS3(ThrowingRunnable action) throws Exception {
        startS3();
        try {
            action.run();
        } finally {
            stopS3();
        }
    }

    @FunctionalInterface
    protected interface ThrowingRunnable {
        void run() throws Exception;
    }

    protected void startS3() {
        s3 = new S3Mock.Builder().withPort(S3_PORT).withInMemoryBackend().build();
        s3.start();
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
            .withPathStyleAccessEnabled(true)
            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(S3_ADDRESS, "us-west-2"))
            .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
            .build();
        s3Client.createBucket(BUCKET_NAME);
    }

    protected void stopS3() {
        s3.shutdown();
        s3 = null;
    }

    protected void startServant(String servantId) throws URISyntaxException, IOException {
        var servant = new LzyServant(LzyAgentConfig.builder()
            .serverAddress(URI.create("grpc://localhost:" + server.port()))
            .whiteboardAddress(URI.create("grpc://localhost:" + rollPort()))
            .channelManagerAddress(URI.create("grpc://localhost:" + channelManager.port()))
            .servantId(servantId)
            .token("token_" + servantId)
            .bucket("bucket_" + servantId)
            .scheme("servant")
            .agentHost("localhost")
            .agentPort(rollPort())
            .fsPort(rollPort())
            .root(Path.of("/tmp/lzy_" + servantId + "/"))
            .build());
        servants.put(servantId, servant);
    }

    protected void createChannel(String name) {
        channelManager.create(makeCreateDirectChannelCommand(UUID.randomUUID().toString(), name),
            SuccessStreamObserver.wrap(
                status -> System.out.println("Channel '" + name + "' created: " + JsonUtils.printSingleLine(status))));
    }

    protected void destroyChannel(String name) {
        channelManager.destroy(makeDestroyChannelCommand(name), SuccessStreamObserver.wrap(
            status -> System.out.println("Channel '" + name + "' removed: " + JsonUtils.printSingleLine(status))));
    }

    protected ArrayBlockingQueue<Object> readPortalSlot(String channelName) {
        var outputSlotRef = Objects.requireNonNull(channelManager.get(channelName)).outputSlot;
        var portalSlot = outputSlotRef.get();
        int n = 100;
        while (portalSlot == null && n-- > 0) {
            LockSupport.parkNanos(Duration.ofMillis(10).toNanos());
            portalSlot = outputSlotRef.get();
        }

        Assert.assertNotNull(portalSlot);

        var iter = server.portal().openOutputSlot(GrpcConverter.from(portalSlot.getSlotInstance()));

        var values = new ArrayBlockingQueue<>(100);

        ForkJoinPool.commonPool().execute(() -> {
            try {
                iter.forEachRemaining(message -> {
                    System.out.println(" ::: got " + JsonUtils.printSingleLine(message));
                    switch (message.getMessageCase()) {
                        case CONTROL -> {
                            if (LzyFsApi.Message.Controls.EOS != message.getControl()) {
                                values.offer(new AssertionError(JsonUtils.printSingleLine(message)));
                            }
                        }
                        case CHUNK -> values.offer(message.getChunk().toStringUtf8());
                        default -> values.offer(new AssertionError(JsonUtils.printSingleLine(message)));
                    }
                });
            } catch (Exception e) {
                values.offer(e);
            }
        });

        return values;
    }
}
