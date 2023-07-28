package ai.lzy.test;

import ai.lzy.storage.StorageClient;
import ai.lzy.storage.StorageClientFactory;
import ai.lzy.test.GrpcUtils;
import ai.lzy.v1.common.LMST;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import io.findify.s3mock.S3Mock;
import org.junit.After;
import org.junit.Before;
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
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;

public class StorageClientTest {
    String address;
    S3Mock s3Storage;
    Path storageTestDir;
    StorageClientFactory storageClientFactory;

    String bucket = "storage-test";
    // ~60MB size
    long fileSize = 1024 * 1024 * 64;

    @Before
    public void setUp() {
        int port = GrpcUtils.rollPort();
        address = "http://localhost:" + port;
        storageTestDir = Path.of(System.getProperty("user.dir"), "lzy-storage-tests").toAbsolutePath();

        s3Storage = new S3Mock.Builder().withPort(port).withFileBackend(storageTestDir.toString()).build();
        s3Storage.start();

        storageClientFactory = new StorageClientFactory(2, 2);

        var credentials = new AnonymousAWSCredentials();
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
            .withPathStyleAccessEnabled(true)
            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(address, "us-west-2"))
            .withCredentials(new AWSStaticCredentialsProvider(credentials))
            .build();
        s3Client.createBucket(bucket);
    }

    @After
    public void tearDown() throws IOException {
        storageClientFactory.destroy();
        s3Storage.shutdown();
        clearDir(storageTestDir);
    }

    StorageClient getClient() {
        return storageClientFactory.provider(LMST.StorageConfig.newBuilder().setS3(
            LMST.S3Credentials.newBuilder().setEndpoint(address).setAccessToken("").setSecretToken("")).build()).get();
    }

    void clearDir(Path path) throws IOException {
        Files.walkFileTree(path, new SimpleFileVisitor<>() {
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
            file.setLength(fileSize);
        }
    }

    @Test
    public void testMultipleReaders() throws IOException, InterruptedException {
        var files = List.of("test-file-0", "test-file-1");
        var storageClient = getClient();

        for (String filename : files) {
            createSparseFile(filename);
            storageClient.write(URI.create("s3://" + bucket + "/" + filename), storageTestDir.resolve(filename));
        }

        System.out.println("Starting download...");

        for (String filename : files) {
            storageClient.read(URI.create("s3://" + bucket + "/" + filename), storageTestDir.resolve(filename));
        }

        for (String filename : files) {
            assertEquals(fileSize, Files.size(storageTestDir.resolve(filename)));
        }
    }

    @Test
    public void testMultipleConcurrentReadersWithOneClient() throws IOException, InterruptedException {
        var files = List.of("test-file-0", "test-file-1", "test-file-2");
        var storageClient = getClient();

        for (String filename : files) {
            createSparseFile(filename);
            storageClient.write(URI.create("s3://" + bucket + "/" + filename), storageTestDir.resolve(filename));
        }

        System.out.println("Starting download...");

        int n = files.size();
        var readyLatch = new CountDownLatch(n);
        var doneLatch = new CountDownLatch(n);
        var executor = Executors.newFixedThreadPool(n);
        var failed = new AtomicBoolean(false);

        for (String filename : files) {
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

    @Test
    public void testMultipleConcurrentReadersWithMultipleClient() throws IOException, InterruptedException {
        var files = List.of("test-file-0", "test-file-1", "test-file-2");

        for (String filename : files) {
            createSparseFile(filename);
            getClient().write(URI.create("s3://" + bucket + "/" + filename), storageTestDir.resolve(filename));
        }

        System.out.println("Starting download...");

        int n = files.size();
        var readyLatch = new CountDownLatch(n);
        var doneLatch = new CountDownLatch(n);
        var executor = Executors.newFixedThreadPool(n);
        var failed = new AtomicBoolean(false);

        for (String filename : files) {
            executor.submit(() -> {
                try {
                    readyLatch.countDown();
                    readyLatch.await();

                    getClient().read(URI.create("s3://" + bucket + "/" + filename), storageTestDir.resolve(filename));
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
