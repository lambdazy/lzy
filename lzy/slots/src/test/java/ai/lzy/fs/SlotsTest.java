package ai.lzy.fs;

import ai.lzy.fs.backends.*;
import ai.lzy.v1.channel.v2.LCMS;
import ai.lzy.v1.channel.v2.LzyChannelManagerGrpc;
import ai.lzy.v1.common.LC;
import ai.lzy.v1.common.LMS;
import ai.lzy.v1.common.LMST;
import ai.lzy.v1.slots.v2.LSA;
import ai.lzy.v1.slots.v2.LzySlotsApiGrpc;
import com.adobe.testing.s3mock.junit4.S3MockRule;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3URI;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.apache.commons.compress.utils.SeekableInMemoryByteChannel;
import org.junit.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

public class SlotsTest {
    private static final String FS_ROOT = "/tmp/lzy_fs_test";
    private static final String ADDRESS = "localhost:1234";
    private static final String EXECUTION_ID = "execution-id";

    private static SlotsService slotsService;
    private static SlotsExecutionContext executionContext;
    private static ChannelManagerMock channelManagerMock;
    private static Server server;
    private static ManagedChannel channel;
    private static LzySlotsApiGrpc.LzySlotsApiBlockingStub slotsStub;

    @ClassRule
    public static S3MockRule s3MockRule = S3MockRule.builder()
        .withHttpPort(12345)
        .silent()
        .build();
    private static AmazonS3 s3Client;

    @BeforeClass
    public static void setUp() throws IOException {
        Files.createDirectories(Paths.get(FS_ROOT));
        channelManagerMock = new ChannelManagerMock();
        slotsService = new SlotsService();

        server = ServerBuilder
            .forPort(1234)
            .addService(channelManagerMock)
            .addService(slotsService)
            .build();
        server.start();

        channel = ManagedChannelBuilder
            .forTarget(ADDRESS)
            .usePlaintext()
            .build();

        var stub = LzyChannelManagerGrpc.newBlockingStub(channel);

        executionContext = new SlotsExecutionContext(
            Path.of(FS_ROOT), List.of(), Map.of(), stub, EXECUTION_ID, ADDRESS, () -> "", slotsService);

        slotsStub = LzySlotsApiGrpc.newBlockingStub(channel);

        s3Client = AmazonS3ClientBuilder.standard()
            .withPathStyleAccessEnabled(true)
            .withEndpointConfiguration(
                new AwsClientBuilder.EndpointConfiguration("http://localhost:12345", "us-west-1"))
            .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
            .build();
    }

    @AfterClass
    public static void tearDown() throws InterruptedException {
        channel.shutdown();
        server.shutdown();
        server.awaitTermination();
    }

    @After
    public void after() {
        executionContext.close();
    }

    @Test
    public void testSimple() throws IOException, InterruptedException {
        var pipePath = Path.of("/tmp", "lzy", "test_simple-out");

        var outBackand = new OutputPipeBackend(pipePath);
        var outHandle = channelManagerMock.onBind("1");

        var outSlot = new OutputSlot(outBackand, "1", "chan", executionContext.context());
        executionContext.addSlot(outSlot);
        var beforeFut = outSlot.beforeExecution();

        Files.write(pipePath, "Hello".getBytes(), StandardOpenOption.WRITE);
        var req = outHandle.get();
        Assert.assertEquals("1", req.getPeerId());

        outHandle.complete(LCMS.BindResponse.getDefaultInstance());

        beforeFut.join();

        var inHandle = channelManagerMock.onBind("2");
        var transferCompletedHandle = channelManagerMock.onTransferCompleted("transfer-id");

        inHandle.complete(LCMS.BindResponse.newBuilder()
            .setPeer(LC.PeerDescription.newBuilder()
                .setPeerId("1")
                .setSlotPeer(LC.PeerDescription.SlotPeer.newBuilder()
                    .setPeerUrl(ADDRESS)
                    .build())
                .build())
            .setTransferId("transfer-id")
            .build()
        );
        var inBackend = new FileInputBackend(Path.of("/tmp", "lzy", "test_simple-in"));

        var inSlot = new InputSlot(inBackend, "2", "chan", executionContext.context());
        executionContext.addSlot(inSlot);
        var inBeforeFut = inSlot.beforeExecution();

        var inReq = inHandle.get();
        Assert.assertEquals("2", inReq.getPeerId());

        transferCompletedHandle.get();
        var bindHandle = channelManagerMock.onBind("2-out");
        transferCompletedHandle.complete(LCMS.TransferCompletedResponse.getDefaultInstance());

        inBeforeFut.join();

        Assert.assertEquals("Hello", Files.readString(Path.of("/tmp", "lzy", "test_simple-in")));

        // Awaiting other side of output slot
        var bindRequest = bindHandle.get();
        bindHandle.complete(LCMS.BindResponse.getDefaultInstance());
    }

    @Test
    public void testCannotBind() throws IOException {
        var path = Path.of("/tmp", "test_cannot_bind-out");
        path.toFile().createNewFile();
        var fileOutputBackend = new OutputFileBackend(path);

        var handle = channelManagerMock.onBind("1");
        var slot = new OutputSlot(fileOutputBackend, "1", "chan", executionContext.context());

        handle.get();
        handle.completeExceptionally(new RuntimeException("Cannot bind"));

        Assert.assertThrows(ExecutionException.class, () -> slot.afterExecution().get());

        var inPath = Path.of("/tmp", "test_cannot_bind-in");
        var inHandle = channelManagerMock.onBind("2");
        var inSlot = new InputSlot(new FileInputBackend(inPath), "2", "chan", executionContext.context());

        inHandle.get();
        inHandle.completeExceptionally(new RuntimeException("Cannot bind"));

        Assert.assertThrows(ExecutionException.class, () -> inSlot.beforeExecution().get());
    }

    @Test
    public void testConnect() throws Exception {
        var inBack = new InMemBackand(new byte[1024]);
        var outBack = new InMemBackand("Hello".getBytes());

        var inBind = channelManagerMock.onBind("1");
        var outBind = channelManagerMock.onBind("2");

        var inSlot = new InputSlot(inBack, "1", "chan", executionContext.context());
        var outSlot = new OutputSlot(outBack, "2", "chan", executionContext.context());

        inBind.get();
        inBind.complete(LCMS.BindResponse.getDefaultInstance());

        outBind.get();
        outBind.complete(LCMS.BindResponse.getDefaultInstance());

        var transferCompletedHandle = channelManagerMock.onTransferCompleted("transfer-id");
        slotsStub.startTransfer(LSA.StartTransferRequest.newBuilder()
            .setSlotId("1")
            .setTransferId("transfer-id")
            .setPeer(LC.PeerDescription.newBuilder()
                .setPeerId("2")
                .setSlotPeer(LC.PeerDescription.SlotPeer.newBuilder()
                    .setPeerUrl(ADDRESS)
                    .build())
                .build())
            .build()
        );

        transferCompletedHandle.get();
        transferCompletedHandle.complete(LCMS.TransferCompletedResponse.getDefaultInstance());

        inSlot.beforeExecution().get();
        Assert.assertTrue(new String(inBack.data, StandardCharsets.UTF_8).startsWith("Hello"));
    }

    @Test
    public void testFailOnInputBackend() {
        var inBack = new InMemBackand(new byte[1024]);
        inBack.failOpen.set(true);

        var outBack = new InMemBackand("Hello".getBytes());

        var inBind = channelManagerMock.onBind("1");
        var outBind = channelManagerMock.onBind("2");

        var inSlot = new InputSlot(inBack, "1", "chan", executionContext.context());
        var outSlot = new OutputSlot(outBack, "2", "chan", executionContext.context());

        inBind.get();
        inBind.complete(LCMS.BindResponse.getDefaultInstance());

        outBind.get();
        outBind.complete(LCMS.BindResponse.getDefaultInstance());

        var transferFailedHandle = channelManagerMock.onTransferFailed("transfer-id");
        slotsStub.startTransfer(LSA.StartTransferRequest.newBuilder()
            .setSlotId("1")
            .setTransferId("transfer-id")
            .setPeer(LC.PeerDescription.newBuilder()
                .setPeerId("2")
                .setSlotPeer(LC.PeerDescription.SlotPeer.newBuilder()
                    .setPeerUrl(ADDRESS)
                    .build())
                .build())
            .build()
        );

        transferFailedHandle.get();
        transferFailedHandle.complete(LCMS.TransferFailedResponse.getDefaultInstance());

        Assert.assertThrows(ExecutionException.class, () -> inSlot.beforeExecution().get());
    }

    @Test
    public void testFailOnOutputBackand() {
        var inBack = new InMemBackand(new byte[1024]);
        var outBack = new InMemBackand("Hello".getBytes());
        outBack.failRead.set(true);

        var inBind = channelManagerMock.onBind("1");
        var outBind = channelManagerMock.onBind("2");

        var inSlot = new InputSlot(inBack, "1", "chan", executionContext.context());
        var outSlot = new OutputSlot(outBack, "2", "chan", executionContext.context());

        inBind.get();
        inBind.complete(LCMS.BindResponse.getDefaultInstance());

        outBind.get();
        outBind.complete(LCMS.BindResponse.getDefaultInstance());

        var transferFailedHandle = channelManagerMock.onTransferFailed("transfer-id");
        slotsStub.startTransfer(LSA.StartTransferRequest.newBuilder()
            .setSlotId("1")
            .setTransferId("transfer-id")
            .setPeer(LC.PeerDescription.newBuilder()
                .setPeerId("2")
                .setSlotPeer(LC.PeerDescription.SlotPeer.newBuilder()
                    .setPeerUrl(ADDRESS)
                    .build())
                .build())
            .build()
        );

        transferFailedHandle.get();
        transferFailedHandle.complete(LCMS.TransferFailedResponse.getDefaultInstance());

        Assert.assertThrows(ExecutionException.class, () -> inSlot.beforeExecution().get());
    }

    @Test
    public void testInputRestart() throws ExecutionException, InterruptedException {
        var inBack = new InMemBackand(new byte[1024]);
        inBack.failOpen.set(true);

        var outBack = new InMemBackand("Hello".getBytes());

        var inBind = channelManagerMock.onBind("1");
        var outBind = channelManagerMock.onBind("2");

        var inSlot = new InputSlot(inBack, "1", "chan", executionContext.context());
        var outSlot = new OutputSlot(outBack, "2", "chan", executionContext.context());

        inBind.get();
        inBind.complete(LCMS.BindResponse.getDefaultInstance());

        outBind.get();
        outBind.complete(LCMS.BindResponse.getDefaultInstance());

        var transferFailedHandle = channelManagerMock.onTransferFailed("transfer-id");
        slotsStub.startTransfer(LSA.StartTransferRequest.newBuilder()
            .setSlotId("1")
            .setTransferId("transfer-id")
            .setPeer(LC.PeerDescription.newBuilder()
                .setPeerId("2")
                .setSlotPeer(LC.PeerDescription.SlotPeer.newBuilder()
                    .setPeerUrl(ADDRESS)
                    .build())
                .build())
            .build()
        );

        transferFailedHandle.get();
        inBack.failOpen.set(false);

        var transferCompletedHandle = channelManagerMock.onTransferCompleted("transfer-id");
        transferFailedHandle.complete(LCMS.TransferFailedResponse.newBuilder()
            .setNewTransferId("transfer-id")
            .setNewPeer(LC.PeerDescription.newBuilder()
                .setPeerId("2")
                .setSlotPeer(LC.PeerDescription.SlotPeer.newBuilder()
                    .setPeerUrl(ADDRESS)
                    .build())
                .build())
            .build());

        transferCompletedHandle.get();
        transferCompletedHandle.complete(LCMS.TransferCompletedResponse.getDefaultInstance());

        inSlot.beforeExecution().get();

        Assert.assertTrue(new String(inBack.data, StandardCharsets.UTF_8).startsWith("Hello"));
    }

    @Test
    public void testReadFromStorage() throws ExecutionException, InterruptedException {
        s3Client.createBucket("bucket-read");
        writeToS3("s3://bucket-read/key1", "Hello");

        var inBack = new InMemBackand(new byte[1024]);
        var inHandle = channelManagerMock.onBind("1");
        var inSlot = new InputSlot(inBack, "1", "chan", executionContext.context());

        inHandle.get();

        var transferHandle = channelManagerMock.onTransferCompleted("transfer-id");

        inHandle.complete(LCMS.BindResponse.newBuilder()
            .setPeer(LC.PeerDescription.newBuilder()
                .setStoragePeer(LC.PeerDescription.StoragePeer.newBuilder()
                    .setS3(LMST.S3Credentials.newBuilder()
                        .setEndpoint("http://localhost:12345")
                        .build())
                    .setStorageUri("s3://bucket-read/key1")
                    .build())
                .build())
                .setTransferId("transfer-id")
            .build());

        transferHandle.get();
        transferHandle.complete(LCMS.TransferCompletedResponse.getDefaultInstance());

        inSlot.beforeExecution().get();

        Assert.assertTrue(new String(inBack.data, StandardCharsets.UTF_8).startsWith("Hello"));
    }

    @Test
    public void testWriteToStorage() throws ExecutionException, InterruptedException, IOException {
        s3Client.createBucket("bucket-write");

        var outBack = new InMemBackand("Hello".getBytes());
        var outHandle = channelManagerMock.onBind("1");
        var outSlot = new OutputSlot(outBack, "1", "chan", executionContext.context());

        outHandle.get();

        var transferHandle = channelManagerMock.onTransferCompleted("transfer-id");

        outHandle.complete(LCMS.BindResponse.newBuilder()
            .setPeer(LC.PeerDescription.newBuilder()
                .setStoragePeer(LC.PeerDescription.StoragePeer.newBuilder()
                    .setS3(LMST.S3Credentials.newBuilder()
                        .setEndpoint("http://localhost:12345")
                        .build())
                    .setStorageUri("s3://bucket-write/key1")
                    .build())
                .build())
                .setTransferId("transfer-id")
            .build());

        transferHandle.get();
        transferHandle.complete(LCMS.TransferCompletedResponse.getDefaultInstance());

        outSlot.afterExecution().get();

        Assert.assertEquals("Hello", readFromS3("s3://bucket-write/key1"));
    }

    @Test
    public void testReadFromStorageFail() throws ExecutionException, InterruptedException {
        var inBack = new InMemBackand(new byte[1024]);
        var inHandle = channelManagerMock.onBind("1");
        var inSlot = new InputSlot(inBack, "1", "chan", executionContext.context());

        inHandle.get();

        var transferHandle = channelManagerMock.onTransferFailed("transfer-id");

        inHandle.complete(LCMS.BindResponse.newBuilder()
            .setPeer(LC.PeerDescription.newBuilder()
                .setStoragePeer(LC.PeerDescription.StoragePeer.newBuilder()
                    .setS3(LMST.S3Credentials.newBuilder()
                        .setEndpoint("http://localhost:12345")
                        .build())
                    .setStorageUri("s3://lolkek/key1")
                    .build())
                .build())
                .setTransferId("transfer-id")
            .build());

        transferHandle.get();
        transferHandle.complete(LCMS.TransferFailedResponse.getDefaultInstance());

        Assert.assertThrows(ExecutionException.class, () -> inSlot.beforeExecution().get());
    }

    @Test
    public void testWriteToStorageFail() {
        var outBack = new InMemBackand("Hello".getBytes());
        var outHandle = channelManagerMock.onBind("1");
        var outSlot = new OutputSlot(outBack, "1", "chan", executionContext.context());

        outHandle.get();

        var transferHandle = channelManagerMock.onTransferFailed("transfer-id");

        outHandle.complete(LCMS.BindResponse.newBuilder()
            .setPeer(LC.PeerDescription.newBuilder()
                .setStoragePeer(LC.PeerDescription.StoragePeer.newBuilder()
                    .setS3(LMST.S3Credentials.newBuilder()
                        .setEndpoint("http://localhost:12345")
                        .build())
                    .setStorageUri("s3://lolkek/key1")
                    .build())
                .build())
                .setTransferId("transfer-id")
            .build());

        transferHandle.get();
        transferHandle.complete(LCMS.TransferFailedResponse.getDefaultInstance());

        Assert.assertThrows(ExecutionException.class, () -> outSlot.afterExecution().get());
    }

    @Test
    public void testExecutionContext() throws Exception {
        s3Client.createBucket("bucket-execution-context");
        writeToS3("s3://bucket-execution-context/key1", "Hello");

        var slots = List.of(
            LMS.Slot.newBuilder()
                .setName("a/b/in")
                .setDirection(LMS.Slot.Direction.INPUT)
                .build(),

            LMS.Slot.newBuilder()
                .setName("a/b/out")
                .setDirection(LMS.Slot.Direction.OUTPUT)
                .build()
        );

        var inHandle = channelManagerMock.onBind("a/b/in");
        inHandle.complete(LCMS.BindResponse.newBuilder()
            .setTransferId("transfer-id1")
            .setPeer(LC.PeerDescription.newBuilder()
                .setStoragePeer(LC.PeerDescription.StoragePeer.newBuilder()
                    .setS3(LMST.S3Credentials.newBuilder()
                        .setEndpoint("http://localhost:12345")
                        .build())
                    .setStorageUri("s3://bucket-execution-context/key1")
                    .build())
                .build())
            .build());

        var outHandle = channelManagerMock.onBind("a/b/out");
        outHandle.complete(LCMS.BindResponse.newBuilder()
            .setTransferId("transfer-id2")
            .setPeer(LC.PeerDescription.newBuilder()
                .setStoragePeer(LC.PeerDescription.StoragePeer.newBuilder()
                    .setS3(LMST.S3Credentials.newBuilder()
                        .setEndpoint("http://localhost:12345")
                        .build())
                    .setStorageUri("s3://bucket-execution-context/key2")
                    .build())
                .build())
            .build());

        var outHandle2 = channelManagerMock.onBind("a/b/in-out");
        outHandle2.complete(LCMS.BindResponse.newBuilder()
            .setTransferId("transfer-id3")
            .setPeer(LC.PeerDescription.newBuilder()
                .setStoragePeer(LC.PeerDescription.StoragePeer.newBuilder()
                    .setS3(LMST.S3Credentials.newBuilder()
                        .setEndpoint("http://localhost:12345")
                        .build())
                    .setStorageUri("s3://bucket-execution-context/key3")
                    .build())
                .build())
            .build());

        var transferHandle1 = channelManagerMock.onTransferCompleted("transfer-id1");
        transferHandle1.complete(LCMS.TransferCompletedResponse.getDefaultInstance());

        var transferHandle2 = channelManagerMock.onTransferCompleted("transfer-id2");
        transferHandle2.complete(LCMS.TransferCompletedResponse.getDefaultInstance());

        var transferHandle3 = channelManagerMock.onTransferCompleted("transfer-id3");
        transferHandle3.complete(LCMS.TransferCompletedResponse.getDefaultInstance());

        var ctx = new SlotsExecutionContext(
            Path.of("/tmp/lzy"),
            slots,
            Map.of(
                "a/b/in", "chan",
                "a/b/out", "chan"
            ),
            LzyChannelManagerGrpc.newBlockingStub(channel),
            "exec",
            ADDRESS,
            () -> "",
            slotsService
        );

        ctx.beforeExecution();
        inHandle.get();
        transferHandle1.get();

        Assert.assertEquals("Hello", Files.readString(Path.of("/tmp/lzy/a/b/in")));

        Files.write(Path.of("/tmp/lzy/a/b/out"), "World".getBytes());
        outHandle.get();
        transferHandle2.get();
        outHandle2.get();
        transferHandle3.get();

        ctx.afterExecution();

        Assert.assertEquals("World", readFromS3("s3://bucket-execution-context/key2"));
        Assert.assertEquals("Hello", readFromS3("s3://bucket-execution-context/key3"));

        ctx.close();
    }

    private static class InMemBackand implements OutputSlotBackend, InputSlotBackend {
        private final byte[] data;
        private final AtomicBoolean failOpen = new AtomicBoolean(false);
        private final AtomicBoolean failRead = new AtomicBoolean(false);
        private final AtomicBoolean failWaitCompleted = new AtomicBoolean(false);

        public InMemBackand(byte[] data) {
            this.data = data;
        }

        @Override
        public SeekableByteChannel openChannel() throws IOException {
            if (failOpen.get()) {
                throw new IOException("Cannot open channel");
            }
            return new SeekableInMemoryByteChannel(data);
        }

        @Override
        public OutputSlotBackend toOutput() {
            return this;
        }

        @Override
        public void waitCompleted() {
            if (failWaitCompleted.get()) {
                throw new RuntimeException("Cannot wait");
            }
        }

        @Override
        public InputStream readFromOffset(long offset) throws IOException {
            if (failRead.get()) {
                throw new IOException("Cannot read");
            }
            var stream = new ByteArrayInputStream(data);
            stream.skip(offset);
            return stream;
        }

        @Override
        public void close() throws IOException {

        }
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
