package ai.lzy.fs;

import ai.lzy.fs.backends.*;
import ai.lzy.model.utils.FreePortFinder;
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
import com.amazonaws.services.s3.model.GetObjectRequest;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.apache.commons.compress.utils.SeekableInMemoryByteChannel;
import org.junit.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class SlotsTest {
    private static final int s3MockPort = FreePortFinder.find(1000, 2000);
    private static final int serverPort = FreePortFinder.find(2000, 3000);
    private static final String FS_ROOT = "/tmp/lzy_fs_test";
    private static final String ADDRESS = "localhost:" + serverPort;
    private static final String S3_ADDRESS = "http://localhost:" + s3MockPort;
    private static final String EXECUTION_ID = "execution-id";

    private static SlotsService slotsService;
    private static SlotsExecutionContext executionContext;
    private static ChannelManagerMock channelManagerMock;
    private static Server server;
    private static ManagedChannel channel;
    private static LzySlotsApiGrpc.LzySlotsApiBlockingStub slotsStub;

    @ClassRule
    public static S3MockRule s3MockRule = S3MockRule.builder()
        .withHttpPort(s3MockPort)
        .silent()
        .build();
    private static AmazonS3 s3Client;

    @BeforeClass
    public static void setUp() throws IOException {
        Files.createDirectories(Paths.get(FS_ROOT));
        channelManagerMock = new ChannelManagerMock();
        slotsService = new SlotsService();

        server = ServerBuilder
            .forPort(serverPort)
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
                new AwsClientBuilder.EndpointConfiguration("http://localhost:" + s3MockPort, "us-west-1"))
            .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
            .build();
        Files.createDirectories(Path.of("/tmp", "lzy"));
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

        var outBackend = new OutputPipeBackend(pipePath);
        var outHandle = channelManagerMock.onBind("1");

        var outSlot = new OutputSlot(outBackend, "1", "chan", executionContext.context());
        executionContext.add(outSlot);
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
        executionContext.add(inSlot);
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
        inSlot.close();
        outSlot.close();
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
        inSlot.close();
    }

    @Test
    public void testConnect() throws Exception {
        var inBack = new InMemBackend(new byte[1024]);
        var outBack = new InMemBackend("Hello".getBytes());

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
        inSlot.close();
        outSlot.close();
    }

    @Test
    public void testFailOnInputBackend() {
        var inBack = new InMemBackend(new byte[1024]);
        inBack.failOpen.set(true);

        var outBack = new InMemBackend("Hello".getBytes());

        var inBind = channelManagerMock.onBind("1");
        var outBind = channelManagerMock.onBind("2");

        var inSlot = new InputSlot(inBack, "1", "chan", executionContext.context());
        var outSlot = new OutputSlot(outBack, "2", "chan", executionContext.context());

        inBind.get();
        inBind.complete(LCMS.BindResponse.getDefaultInstance());

        outBind.get();
        outBind.complete(LCMS.BindResponse.getDefaultInstance());

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

        Assert.assertThrows(ExecutionException.class, () -> inSlot.beforeExecution().get());
        inSlot.close();
        outSlot.close();
    }

    @Test
    public void testFailOnOutputBackend() {
        var inBack = new InMemBackend(new byte[1024]);
        var outBack = new InMemBackend("Hello".getBytes());
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
        inSlot.close();
        outSlot.close();
    }

    @Test
    public void testInputRestart() throws ExecutionException, InterruptedException {
        var inBack = new InMemBackend(new byte[1024]);

        var outBack = new InMemBackend("Hello".getBytes());
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
        outBack.failRead.set(false);

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
        inSlot.close();
        outSlot.close();
    }

    @Test
    public void testReadFromStorage() throws ExecutionException, InterruptedException {
        s3Client.createBucket("bucket-read");
        writeToS3("s3://bucket-read/key1", "Hello");

        var inBack = new InMemBackend(new byte[1024]);
        var inHandle = channelManagerMock.onBind("1");
        var inSlot = new InputSlot(inBack, "1", "chan", executionContext.context());

        inHandle.get();

        var transferHandle = channelManagerMock.onTransferCompleted("transfer-id");

        inHandle.complete(LCMS.BindResponse.newBuilder()
            .setPeer(LC.PeerDescription.newBuilder()
                .setStoragePeer(LC.PeerDescription.StoragePeer.newBuilder()
                    .setS3(LMST.S3Credentials.newBuilder()
                        .setEndpoint(S3_ADDRESS)
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
        inSlot.close();
    }

    @Test
    public void testWriteToStorage() throws ExecutionException, InterruptedException, IOException {
        s3Client.createBucket("bucket-write");

        var outBack = new InMemBackend("Hello".getBytes());
        var outHandle = channelManagerMock.onBind("1");
        var outSlot = new OutputSlot(outBack, "1", "chan", executionContext.context());

        outHandle.get();

        var transferHandle = channelManagerMock.onTransferCompleted("transfer-id");

        outHandle.complete(LCMS.BindResponse.newBuilder()
            .setPeer(LC.PeerDescription.newBuilder()
                .setStoragePeer(LC.PeerDescription.StoragePeer.newBuilder()
                    .setS3(LMST.S3Credentials.newBuilder()
                        .setEndpoint(S3_ADDRESS)
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
        outSlot.close();
    }

    @Test
    public void testReadFromStorageFail() throws ExecutionException, InterruptedException {
        var inBack = new InMemBackend(new byte[1024]);
        var inHandle = channelManagerMock.onBind("1");
        var inSlot = new InputSlot(inBack, "1", "chan", executionContext.context());

        inHandle.get();

        var transferHandle = channelManagerMock.onTransferFailed("transfer-id");

        inHandle.complete(LCMS.BindResponse.newBuilder()
            .setPeer(LC.PeerDescription.newBuilder()
                .setStoragePeer(LC.PeerDescription.StoragePeer.newBuilder()
                    .setS3(LMST.S3Credentials.newBuilder()
                        .setEndpoint(S3_ADDRESS)
                        .build())
                    .setStorageUri("s3://lolkek/key1")
                    .build())
                .build())
                .setTransferId("transfer-id")
            .build());

        transferHandle.get();
        transferHandle.complete(LCMS.TransferFailedResponse.getDefaultInstance());

        Assert.assertThrows(ExecutionException.class, () -> inSlot.beforeExecution().get());
        inSlot.close();
    }

    @Test
    public void testWriteToStorageFail() {
        var outBack = new InMemBackend("Hello".getBytes());
        var outHandle = channelManagerMock.onBind("1");
        var outSlot = new OutputSlot(outBack, "1", "chan", executionContext.context());

        outHandle.get();

        var transferHandle = channelManagerMock.onTransferFailed("transfer-id");

        outHandle.complete(LCMS.BindResponse.newBuilder()
            .setPeer(LC.PeerDescription.newBuilder()
                .setStoragePeer(LC.PeerDescription.StoragePeer.newBuilder()
                    .setS3(LMST.S3Credentials.newBuilder()
                        .setEndpoint(S3_ADDRESS)
                        .build())
                    .setStorageUri("s3://lolkek/key1")
                    .build())
                .build())
                .setTransferId("transfer-id")
            .build());

        transferHandle.get();
        transferHandle.complete(LCMS.TransferFailedResponse.getDefaultInstance());

        Assert.assertThrows(ExecutionException.class, () -> outSlot.afterExecution().get());
        outSlot.close();
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
                        .setEndpoint(S3_ADDRESS)
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
                        .setEndpoint(S3_ADDRESS)
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
                        .setEndpoint(S3_ADDRESS)
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

    @Test
    public void testLargeData() throws Exception {
        var inPath = Path.of("/tmp/lzy/test-large-data-in");
        var outPath = Path.of("/tmp/lzy/test-large-data-out");

        if (Files.exists(outPath)) {
            Files.delete(outPath);
        }

        Files.createFile(outPath);

        var inBack = new FileInputBackend(inPath);
        var outBack = new OutputFileBackend(outPath);

        var genDataTime = System.currentTimeMillis();
        var rand = new Random();
        var data = new byte[1024 * 1024];  // 1 Mb

        // Writing random data of size 1 Gb
        for (int i = 0; i < 1024; i++) {
            rand.nextBytes(data);
            Files.write(outPath, data, StandardOpenOption.APPEND);
        }
        System.out.println("Data generated in " + (System.currentTimeMillis() - genDataTime) + " ms");

        var time = System.currentTimeMillis();
        var inBind = channelManagerMock.onBind("1");
        var outBind = channelManagerMock.onBind("2");

        var inSlot = new InputSlot(inBack, "1", "chan", executionContext.context());
        var outSlot = new OutputSlot(outBack, "2", "chan", executionContext.context());


        outBind.get();
        outBind.complete(LCMS.BindResponse.getDefaultInstance());

        var transferCompletedHandle = channelManagerMock.onTransferCompleted("transfer-id");

        inBind.get();
        inBind.complete(LCMS.BindResponse.newBuilder()
            .setPeer(LC.PeerDescription.newBuilder()
                .setPeerId("2")
                .setSlotPeer(LC.PeerDescription.SlotPeer.newBuilder()
                    .setPeerUrl(ADDRESS)
                    .build())
                .build())
            .setTransferId("transfer-id")
            .build());

        transferCompletedHandle.get();
        transferCompletedHandle.complete(LCMS.TransferCompletedResponse.getDefaultInstance());

        inSlot.beforeExecution().get();

        System.out.println("Data read in " + (System.currentTimeMillis() - time) + " ms");

        Assert.assertEquals(-1, Files.mismatch(inPath, outPath));

        Files.delete(inPath);
        Files.delete(outPath);

        inSlot.close();
        outSlot.close();
    }

    @Test
    public void testLargeReadFromStorage() throws Exception {
        var inPath = Path.of("/tmp/lzy/test-large-storage-in");
        var dataPath = Path.of("/tmp/lzy/test-large-storage-data");

        s3Client.createBucket("bucket-read-large");

        if (Files.exists(dataPath)) {
            Files.delete(dataPath);
        }
        Files.createFile(dataPath);

        var timeToGenData = System.currentTimeMillis();
        var rand = new Random();
        var data = new byte[1024 * 1024];  // 1 Mb
        // Writing random data of size 1 Gb
        for (int i = 0; i < 1024; i++) {
            rand.nextBytes(data);
            Files.write(dataPath, data, StandardOpenOption.APPEND);
        }
        System.out.println("Time to generate data: " + (System.currentTimeMillis() - timeToGenData));

        var time = System.currentTimeMillis();
        s3Client.putObject("bucket-read-large", "key", dataPath.toFile());
        System.out.println("Time to upload: " + (System.currentTimeMillis() - time));

        var timeToRead = System.currentTimeMillis();
        var inBack = new FileInputBackend(inPath);

        var inBind = channelManagerMock.onBind("1");
        var inSlot = new InputSlot(inBack, "1", "chan", executionContext.context());

        var transferCompletedHandle = channelManagerMock.onTransferCompleted("transfer-id");

        inBind.get();
        inBind.complete(LCMS.BindResponse.newBuilder()
            .setPeer(LC.PeerDescription.newBuilder()
                .setStoragePeer(LC.PeerDescription.StoragePeer.newBuilder()
                    .setS3(LMST.S3Credentials.newBuilder()
                        .setEndpoint(S3_ADDRESS)
                        .build())
                    .setStorageUri("s3://bucket-read-large/key")
                    .build())
                .build())
            .setTransferId("transfer-id")
            .build());

        transferCompletedHandle.get();
        transferCompletedHandle.complete(LCMS.TransferCompletedResponse.getDefaultInstance());

        inSlot.beforeExecution().get();
        System.out.println("Time to read: " + (System.currentTimeMillis() - timeToRead));

        Assert.assertEquals(-1, Files.mismatch(inPath, dataPath));

        Files.delete(inPath);
        Files.delete(dataPath);
    }

    @Test
    public void testLargeWriteToStorage() throws Exception {
        var inPath = Path.of("/tmp/lzy/test-large-storage-write-in");
        var dataPath = Path.of("/tmp/lzy/test-large-storage-write-data");
        s3Client.createBucket("bucket-write-large");

        if (Files.exists(inPath)) {
            Files.delete(inPath);
        }
        Files.createFile(inPath);

        var timeToGenData = System.currentTimeMillis();
        var rand = new Random();
        var data = new byte[1024 * 1024];  // 1 Mb
        // Writing random data of size 1 Gb
        for (int i = 0; i < 1024; i++) {
            rand.nextBytes(data);
            Files.write(inPath, data, StandardOpenOption.APPEND);
        }
        System.out.println("Time to generate data: " + (System.currentTimeMillis() - timeToGenData));

        var outBack = new OutputFileBackend(inPath);

        var outBind = channelManagerMock.onBind("1");
        var outSlot = new OutputSlot(outBack, "1", "chan", executionContext.context());

        outBind.get();
        var transferCompletedHandle = channelManagerMock.onTransferCompleted("transfer-id");
        outBind.complete(LCMS.BindResponse.newBuilder()
            .setPeer(LC.PeerDescription.newBuilder()
                .setStoragePeer(LC.PeerDescription.StoragePeer.newBuilder()
                    .setS3(LMST.S3Credentials.newBuilder()
                        .setEndpoint(S3_ADDRESS)
                        .build())
                    .setStorageUri("s3://bucket-write-large/key")
                    .build())
                .build())
            .setTransferId("transfer-id")
            .build());

        transferCompletedHandle.get();
        transferCompletedHandle.complete(LCMS.TransferCompletedResponse.getDefaultInstance());

        s3Client.getObject(new GetObjectRequest("bucket-write-large", "key"), dataPath.toFile());
        Assert.assertEquals(-1, Files.mismatch(inPath, dataPath));
        outSlot.close();
    }

    @Test
    public void testRestoreReadFromOffset() throws Exception {
        var data = new byte[1024 * 1024 * 2];  // 2 Mb
        var rand = new Random();
        rand.nextBytes(data);

        var inBack = new InMemBackend(new byte[1024 * 1024 * 2]);
        var outBack = new InMemBackend(data);

        outBack.failAfter.set(1024 * 1024);  // Fail after first chunk

        var inBind = channelManagerMock.onBind("1");
        var outBind = channelManagerMock.onBind("2");

        var inSlot = new InputSlot(inBack, "1", "chan", executionContext.context());
        var outSlot = new OutputSlot(outBack, "2", "chan", executionContext.context());

        var transferFailedHandle = channelManagerMock.onTransferFailed("transfer-id1");

        inBind.get();
        inBind.complete(LCMS.BindResponse.newBuilder()
            .setPeer(LC.PeerDescription.newBuilder()
                .setPeerId("2")
                .setSlotPeer(LC.PeerDescription.SlotPeer.newBuilder()
                    .setPeerUrl(ADDRESS)
                    .build())
                .build())
            .setTransferId("transfer-id1")
            .build());

        outBind.get();
        outBind.complete(LCMS.BindResponse.getDefaultInstance());

        transferFailedHandle.get();

        Assert.assertArrayEquals(Arrays.copyOfRange(data, 0, 1024 * 1024),
            Arrays.copyOfRange(inBack.data, 0, 1024 * 1024));  // Assert that first chunk was written

        outBack.failAfter.set(-1);

        var transferCompletedHandle = channelManagerMock.onTransferCompleted("transfer-id2");
        transferFailedHandle.complete(LCMS.TransferFailedResponse.newBuilder()
            .setNewPeer(LC.PeerDescription.newBuilder()
                .setPeerId("2")
                .setSlotPeer(LC.PeerDescription.SlotPeer.newBuilder()
                    .setPeerUrl(ADDRESS)
                    .build())
                .build())
            .setNewTransferId("transfer-id2")
            .build());

        transferCompletedHandle.get();
        transferCompletedHandle.complete(LCMS.TransferCompletedResponse.getDefaultInstance());

        inSlot.beforeExecution().get();
        Assert.assertArrayEquals(data, inBack.data);
        inSlot.close();
        outSlot.close();
    }

    private static class InMemBackend implements OutputSlotBackend, InputSlotBackend {
        private final byte[] data;
        private final AtomicBoolean failOpen = new AtomicBoolean(false);
        private final AtomicBoolean failRead = new AtomicBoolean(false);
        private final AtomicBoolean failWaitCompleted = new AtomicBoolean(false);
        private final AtomicLong failAfter = new AtomicLong(-1);

        public InMemBackend(byte[] data) {
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
        public ReadableByteChannel readFromOffset(long offset) throws IOException {
            if (failRead.get()) {
                throw new IOException("Cannot read");
            }

            var stream = new ByteArrayInputStream(data);
            stream.skip(offset);

            return new FailingInputStream(stream, Math.max(0, failAfter.get() - offset));
        }

        @Override
        public void close() throws IOException {

        }
    }

    private static class FailingInputStream implements ReadableByteChannel {
        private final ReadableByteChannel inner;
        private final long failAfter;
        private long position = 0;

        public FailingInputStream(InputStream inner, long failAfter) {
            this.inner = Channels.newChannel(inner);
            this.failAfter = failAfter;
        }

        @Override
        public int read(ByteBuffer dst) throws IOException {
            if (failAfter > 0 && position >= failAfter) {
                throw new IOException("Cannot read");
            }
            var res = inner.read(dst);
            position += res;
            return res;
        }

        @Override
        public boolean isOpen() {
            return true;
        }

        @Override
        public void close() throws IOException {
            inner.close();
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
