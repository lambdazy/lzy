package ai.lzy.fs.snapshot;

import ai.lzy.model.SlotInstance;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import java.io.IOException;

import java.io.*;
import java.net.URI;
import java.nio.channels.Channels;
import java.nio.channels.Pipe;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ai.lzy.model.Slot;
import ai.lzy.fs.storage.StorageClient;
import ru.yandex.qe.s3.transfer.meta.Metadata;
import ru.yandex.qe.s3.transfer.upload.UploadRequestBuilder;
import ru.yandex.qe.s3.transfer.upload.UploadState;

public class SlotSnapshotImpl implements SlotSnapshot {
    private static final Logger LOG = LogManager.getLogger(SlotSnapshot.class);
    private final StorageClient storage;

    private final String bucket;
    private final SlotInstance slot;
    private final AtomicBoolean nonEmpty = new AtomicBoolean(false);
    private final OutputStream out;
    private final Pipe pipe;
    private final ListenableFuture<UploadState> future;

    public SlotSnapshotImpl(String bucket, SlotInstance slot, StorageClient storage) {
        this.bucket = bucket;
        try {
            this.pipe = Pipe.open();
        } catch (IOException e) {
            throw new RuntimeException("Cannot create pipe ", e);
        }
        this.out = Channels.newOutputStream(pipe.sink());
        this.slot = slot;
        this.storage = storage;
        future = storage.transmitter().upload(new UploadRequestBuilder()
            .bucket(bucket)
            .key(generateKey())
            .metadata(Metadata.empty())
            .stream(() -> Channels.newInputStream(pipe.source()))
            .build()
        );
    }

    private String generateKey() {
        return Path.of("task", slot.taskId(), "slot", slot.name()).toString();
    }

    @Override
    public URI uri() {
        return storage.getURI(bucket, generateKey());
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
