package ai.lzy.servant.portal.slots;

import ai.lzy.fs.fs.LzyOutputSlot;
import ai.lzy.fs.slots.LzySlotBase;
import ai.lzy.fs.slots.OutFileSlot;
import ai.lzy.model.SlotInstance;
import ai.lzy.servant.portal.s3.S3Repository;
import com.google.protobuf.ByteString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static ai.lzy.v1.Operations.SlotStatus.State.OPEN;

public class S3StorageOutputSlot extends LzySlotBase implements LzyOutputSlot {
    private static final Logger LOG = LogManager.getLogger(S3StorageOutputSlot.class);

    private final Path localTempStorage;
    private final CompletableFuture<Supplier<FileChannel>> channelSupplier = new CompletableFuture<>();

    private final String key;
    private final String bucket;
    private final S3Repository<Stream<ByteString>> repository;

    public S3StorageOutputSlot(SlotInstance slotInstance, String s3Key, String s3Bucket,
                               S3Repository<Stream<ByteString>> s3Repository) {
        super(slotInstance);
        String suffix = (slotInstance.taskId() + slotInstance.name()).replaceAll(File.separator, "");
        try {
            this.localTempStorage = Files.createTempFile("lzy", suffix);
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
        this.key = s3Key;
        this.bucket = s3Bucket;
        this.repository = s3Repository;
    }

    private static void write(Stream<ByteString> data, File sink) throws IOException {
        try (var storage = new BufferedOutputStream(new FileOutputStream(sink))) {
            data.forEach(chunk -> {
                try {
                    LOG.debug("Received chunk of size {}", chunk.size());
                    chunk.writeTo(storage);
                } catch (IOException ioe) {
                    LOG.warn("Unable write chunk of data of size " + chunk.size()
                            + " to file " + sink.getAbsolutePath(), ioe);
                }
            });
        }
    }

    public void open() {
        try {
            write(repository.get(bucket, key), localTempStorage.toFile());
        } catch (IOException e) {
            LOG.warn("Unable write data from s3-bucket {} to intermediate local file {}", bucket, localTempStorage);
        }
        channelSupplier.complete(() -> { // channels are now ready to read
            try {
                return FileChannel.open(localTempStorage);
            } catch (IOException e) {
                LOG.error("Can not open file channel on file {}", localTempStorage);
                throw new RuntimeException(e);
            }
        });
        state(OPEN);
    }

    @Override
    public synchronized void close() {
        super.close();
        try {
            if (channelSupplier.isDone()) {
                channelSupplier.get().get().close();
            }
        } catch (IOException | ExecutionException | InterruptedException e) {
            LOG.warn("Unable to close channel on file " + localTempStorage);
        }
    }

    @Override
    public Stream<ByteString> readFromPosition(long offset) throws IOException {
        LOG.info("S3StorageOutputSlot.readFromPosition for slot " + this.definition().name()
                + ", current state " + state());
        FileChannel channel;
        waitForState(OPEN);
        if (state() != OPEN) {
            throw new IllegalStateException("Slot is not open, cannot read");
        }
        try {
            channel = channelSupplier.get().get();
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("Cannot obtain file channel on file {}", localTempStorage);
            throw new RuntimeException(e);
        }
        LOG.info("Slot {} is ready", name());
        return OutFileSlot.readFileChannel(name(), offset, channel, () -> state() == OPEN);
    }
}
