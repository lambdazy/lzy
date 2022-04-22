package ru.yandex.cloud.ml.platform.lzy.servant.snapshot;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.URI;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.LzyExecution;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzyInputSlot;
import ru.yandex.cloud.ml.platform.lzy.servant.storage.StorageClient;
import ru.yandex.qe.s3.transfer.StreamSuppliers;
import ru.yandex.qe.s3.transfer.download.DownloadRequestBuilder;
import ru.yandex.qe.s3.transfer.meta.Metadata;
import ru.yandex.qe.s3.transfer.upload.UploadRequestBuilder;
import ru.yandex.qe.s3.transfer.upload.UploadState;
import ru.yandex.qe.s3.util.function.ThrowingConsumer;

public class SlotSnapshotImpl implements SlotSnapshot {
    private static final Logger LOG = LogManager.getLogger(LzyExecution.class);
    private final StorageClient storage;

    private final String taskId;
    private final String bucket;
    private final Slot slot;
    private final AtomicBoolean nonEmpty = new AtomicBoolean(false);
    private final PipedOutputStream out = new PipedOutputStream();
    private final ListenableFuture<UploadState> future;

    public SlotSnapshotImpl(String taskId, String bucket, Slot slot, StorageClient storage) {
        this.bucket = bucket;
        this.taskId = taskId;
        this.slot = slot;
        this.storage = storage;
        future = storage.transmitter().upload(new UploadRequestBuilder()
            .bucket(bucket)
            .key(generateKey(slot))
            .metadata(Metadata.empty())
            .stream(() -> new PipedInputStream(out))
            .build()
        );
    }

    private String generateKey(Slot slot) {
        return Path.of("task", taskId, "slot", slot.name()).toString();
    }

    @Override
    public URI uri() {
        return storage.getURI(bucket, generateKey(slot));
    }

    @Override
    public synchronized void onChunk(ByteString chunk) {
        LOG.info("S3SlotSnapshot::onChunk invoked with slot " + slot.name());
        try {
            out.write(chunk.toByteArray());
            nonEmpty.set(true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isEmpty() {
        return !nonEmpty.get();
    }

    @Override
    public synchronized void onFinish() {
        LOG.info("S3SlotSnapshot::onFinish invoked with slot " + slot.name());
        try {
            out.close();
            future.get();
        } catch (IOException | InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
