package ai.lzy.storage;

import ai.lzy.storage.s3.S3ClientWithTransmitter;
import ai.lzy.test.GrpcUtils;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import io.findify.s3mock.S3Mock;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URI;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;

@Ignore
public class StorageClientTest {
    S3Mock s3Storage;
    Path storageTestDir;
    StorageClient storageClient;
    ExecutorService downloadUploadExecutor;

    String bucket = "storage-test";
    long fileSize = 1024 * 1024 * 128;

    @Before
    public void setUp() {
        int port = GrpcUtils.rollPort();
        String address = "http://localhost:" + port;
        storageTestDir = Path.of(System.getProperty("user.dir"), "lzy-storage-tests").toAbsolutePath();

        s3Storage = new S3Mock.Builder().withPort(port).withFileBackend(storageTestDir.toString()).build();
        s3Storage.start();

        var credentials = new AnonymousAWSCredentials();
        downloadUploadExecutor = Executors.newFixedThreadPool(5);
        storageClient = new S3ClientWithTransmitter(address, "", "", downloadUploadExecutor);

        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
            .withPathStyleAccessEnabled(true)
            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(address, "us-west-2"))
            .withCredentials(new AWSStaticCredentialsProvider(credentials))
            .build();
        s3Client.createBucket(bucket);
    }

    @After
    public void tearDown() throws IOException {
        downloadUploadExecutor.shutdownNow();
        try {
            //noinspection ResultOfMethodCallIgnored
            downloadUploadExecutor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
            // blank
        }

        s3Storage.stop();
        clearDir(storageTestDir);
    }

    void clearDir(Path path) throws IOException {
        Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    void createSparseFile(String filename) throws IOException {
        try (var file = new RandomAccessFile(storageTestDir.resolve(filename).toFile(), "rw")) {
            // file size > 100 MB
            file.setLength(fileSize);
        }
    }

    @Test
    public void testMultipleReaders() throws IOException, InterruptedException {
        var files = List.of(
            "test-file-0", "test-file-1", "test-file-2", "test-file-3", "test-file-4"
        );

        for (String filename : files) {
            createSparseFile(filename);
            storageClient.write(URI.create("s3://" + bucket + "/" + filename), storageTestDir.resolve(filename));
        }
        for (String filename : files) {
            storageClient.read(URI.create("s3://" + bucket + "/" + filename), storageTestDir.resolve(filename));
        }

        for (String filename : files) {
            assertEquals(fileSize, Files.size(storageTestDir.resolve(filename)));
        }
    }

    @Test
    @Ignore("Must not halt")
    public void testMultipleConcurrentReaders() throws IOException, InterruptedException {
        var files = List.of(
            "test-file-0", "test-file-1", "test-file-2", "test-file-3", "test-file-4"
        );

        for (String filename : files) {
            createSparseFile(filename);
            storageClient.write(URI.create("s3://" + bucket + "/" + filename), storageTestDir.resolve(filename));
        }

        int n = 5;
        var readyLatch = new CountDownLatch(n);
        var doneLatch = new CountDownLatch(n);
        var executor = Executors.newFixedThreadPool(n);
        var failed = new AtomicBoolean(false);

        for (int i = 0; i < n; ++i) {
            var filename = files.get(i);
            executor.submit(() -> {
                try {
                    readyLatch.countDown();
                    readyLatch.await();

                    storageClient.read(URI.create("s3://" + bucket + "/" + filename), storageTestDir.resolve(filename));
                } catch (Exception e) {
                    failed.set(true);
                    e.printStackTrace(System.err);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        doneLatch.await();
        executor.shutdown();

        if (failed.get()) {
            throw new RuntimeException("Some of concurrent call was failed");
        }

        for (String filename : files) {
            assertEquals(fileSize, Files.size(storageTestDir.resolve(filename)));
        }
    }
}
