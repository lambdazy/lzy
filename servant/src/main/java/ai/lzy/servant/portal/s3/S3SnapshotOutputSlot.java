package ai.lzy.servant.portal.s3;

import ai.lzy.fs.fs.LzyOutputSlot;
import ai.lzy.fs.slots.LzySlotBase;
import ai.lzy.fs.slots.OutFileSlot;
import ai.lzy.fs.storage.StorageClient;
import ai.lzy.model.SlotInstance;
import ai.lzy.v1.Operations;
import com.google.protobuf.ByteString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.qe.s3.transfer.download.DownloadRequestBuilder;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class S3SnapshotOutputSlot extends LzySlotBase implements LzyOutputSlot {
    private static final Logger LOG = LogManager.getLogger(S3SnapshotOutputSlot.class);

    private final StorageClient storageClient;
    private final String bucket;

    private final String localStorage;

    private final AtomicBoolean alreadyLocallyStored = new AtomicBoolean(false);

    public S3SnapshotOutputSlot(SlotInstance slotInstance, StorageClient storageClient, String bucket) {
        super(slotInstance);
        this.storageClient = storageClient;
        this.bucket = bucket;
        this.localStorage = "from_s3_%s_storage".formatted(instance().name());
    }

    private String generateKey() {
        return "slot_" + instance().name();
    }

    private void ensureSnapshotLocallyStored() {
        if (alreadyLocallyStored.compareAndSet(false, true)) {
            try {
                Files.createTempFile("lzy", localStorage);
            } catch (IOException e) {
                LOG.error("S3SnapshotOutputSlot:: Failed to create file to store bucket data", e);
                alreadyLocallyStored.set(false);
                return;
            }
            BlockingQueue<ByteString> queue = new ArrayBlockingQueue<>(1000);
            storageClient.transmitter().downloadC(
                    new DownloadRequestBuilder()
                            .bucket(bucket)
                            .key(generateKey())
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
    }

    protected void storeAll(Stream<ByteString> from) {
        try (Stream<ByteString> data = from; OutputStream out = new FileOutputStream(localStorage)) {
            data.forEach(chunk -> {
                try {
                    LOG.debug("From s3-bucket {} received chunk of size {}", bucket, chunk.size());
                    chunk.writeTo(out);
                } catch (IOException ioe) {
                    LOG.warn(
                            "Unable write chunk of data of size " + chunk.size()
                                    + " to portal storage" + localStorage,
                            ioe
                    );
                }
            });
        } catch (Exception e) {
            close();
            return;
        }
        LOG.info("Store data from s3-bucket {} to portal storage {}", bucket, localStorage);
        state(Operations.SlotStatus.State.OPEN);
    }

    @Override
    public Stream<ByteString> readFromPosition(long offset) throws IOException {
        ensureSnapshotLocallyStored();
        var channel = FileChannel.open(Path.of(localStorage), StandardOpenOption.READ);
        return OutFileSlot.readFileChannel(definition().name(), offset, channel, () -> true);
    }
}
