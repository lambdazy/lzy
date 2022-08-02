package ai.lzy.servant.portal.s3;

import ai.lzy.fs.fs.LzyOutputSlot;
import ai.lzy.fs.slots.LzySlotBase;
import ai.lzy.fs.slots.OutFileSlot;
import ai.lzy.fs.storage.AmazonStorageClient;
import ai.lzy.model.SlotInstance;
import ai.lzy.servant.portal.s3.ExternalStorageSupport.AmazonS3Key;
import ai.lzy.v1.LzyPortalApi;
import ai.lzy.v1.LzyPortalApi.AmazonS3Endpoint;
import ai.lzy.v1.Operations;
import com.google.protobuf.ByteString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.qe.s3.transfer.download.DownloadRequestBuilder;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class AmazonS3SnapshotOutputSlot extends LzySlotBase implements LzyOutputSlot {
    private static final Logger LOG = LogManager.getLogger(AmazonS3SnapshotOutputSlot.class);

    private final AmazonStorageClient s3Client;
    private final String s3Bucket;
    private final String keyInBucket;

    private final String storageFilename;

    public AmazonS3SnapshotOutputSlot(SlotInstance slotInstance, String storageFilename, AmazonS3Endpoint s3Endpoint) {
        super(slotInstance);
        this.s3Client = (AmazonStorageClient) AmazonS3Key.of(s3Endpoint.getEndpoint(),
                s3Endpoint.getAccessToken(), s3Endpoint.getSecretToken()).get();
        this.s3Bucket = s3Endpoint.getBucket();
        this.keyInBucket = "slot_%s".formatted(slotInstance.name());
        this.storageFilename = storageFilename;
        ensureBucketExists();
    }

    private void ensureBucketExists() {
        if (!s3Client.doesBucketExist(s3Bucket)) {
            LOG.error("Bucket with name '{}' does not exists", s3Bucket);
            throw new RuntimeException("Bucket with name '" + s3Bucket + "' does not exists");
        }
    }

    private boolean isSnapshotAlreadyStored() {
        File storage = new File(storageFilename);
        return storage.isFile();
    }

    private void ensureSnapshotLocallyStored() {
        if (isSnapshotAlreadyStored()) {
            return;
        }
        ensureBucketExists();

        try {
            Files.createTempFile("lzy", storageFilename);
        } catch (IOException e) {
            LOG.error("AmazonSSnapshotOutputSlot:: Failed to create file to store bucket data", e);
        }
        BlockingQueue<ByteString> queue = new ArrayBlockingQueue<>(1000);
        this.s3Client.transmitter().downloadC(
                new DownloadRequestBuilder()
                        .bucket(s3Bucket)
                        .key(keyInBucket)
                        .build(),
                data -> {
                    final byte[] buffer = new byte[4096];
                    try (final InputStream stream = data.getInputStream()) {
                        int len = 0;
                        while (len != -1) {
                            final ByteString chunk = ByteString.copyFrom(buffer, 0, len);
                            //noinspection StatementWithEmptyBody,CheckStyle
                            while (!queue.offer(chunk, 1, TimeUnit.SECONDS)) {
                            }
                            len = stream.read(buffer);
                        }
                        //noinspection StatementWithEmptyBody,CheckStyle
                        while (!queue.offer(ByteString.EMPTY, 1, TimeUnit.SECONDS)) {
                        }
                    }
                }
        );
        final Iterator<ByteString> chunkIterator = new Iterator<>() {
            ByteString chunk = null;

            @Override
            public boolean hasNext() {
                try {
                    while (chunk == null) {
                        chunk = queue.poll(1, TimeUnit.SECONDS);
                    }
                } catch (InterruptedException ie) {
                    throw new RuntimeException(ie);
                }
                return chunk != ByteString.EMPTY;
            }

            @Override
            public ByteString next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }

                final ByteString chunk = this.chunk;
                this.chunk = null;
                return chunk;
            }
        };

        Stream<ByteString> bucketData = StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(chunkIterator, Spliterator.NONNULL),
                false
        );
        storeAll(bucketData);
    }

    protected void storeAll(Stream<ByteString> from) {
        try (Stream<ByteString> data = from; OutputStream out = new FileOutputStream(storageFilename)) {
            data.forEach(chunk -> {
                try {
                    LOG.debug("From s3-bucket {} received chunk of size {}", s3Bucket, chunk.size());
                    chunk.writeTo(out);
                } catch (IOException ioe) {
                    LOG.warn(
                            "Unable write chunk of data of size " + chunk.size()
                                    + " to portal storage" + storageFilename,
                            ioe
                    );
                }
            });
        } catch (Exception e) {
            close();
            return;
        }
        LOG.info("Store data from s3-bucket {} to portal storage {}", s3Bucket, storageFilename);
        state(Operations.SlotStatus.State.OPEN);
    }

    @Override
    public Stream<ByteString> readFromPosition(long offset) throws IOException {
        ensureSnapshotLocallyStored();
        var channel = FileChannel.open(Path.of(storageFilename), StandardOpenOption.READ);
        return OutFileSlot.readFileChannel(definition().name(), offset, channel, () -> true);
    }
}
