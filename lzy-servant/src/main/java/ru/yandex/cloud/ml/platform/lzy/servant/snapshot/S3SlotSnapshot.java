package ru.yandex.cloud.ml.platform.lzy.servant.snapshot;

import com.amazonaws.services.s3.AmazonS3;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.LzyExecution;
import ru.yandex.cloud.ml.platform.lzy.servant.snapshot.storage.SnapshotStorage;
import ru.yandex.cloud.ml.platform.model.util.lock.LockManager;
import ru.yandex.qe.s3.transfer.Transmitter;
import ru.yandex.qe.s3.transfer.meta.Metadata;
import ru.yandex.qe.s3.transfer.upload.UploadRequestBuilder;
import ru.yandex.qe.s3.transfer.upload.UploadState;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.URI;
import java.util.concurrent.atomic.AtomicBoolean;

public class S3SlotSnapshot implements SlotSnapshot {
    private static final Logger LOG = LogManager.getLogger(LzyExecution.class);
    private final SnapshotStorage storage;

    private final String taskId;
    private final String bucket;
    private final Slot slot;
    private StreamsWrapper slotStream = null;
    private final Lock lock = new ReentrantLock();
    private final AtomicBoolean nonEmpty = new AtomicBoolean(false);
    private final AtomicBoolean bucketInited = new AtomicBoolean(false);

    public S3SlotSnapshot(String taskId, String bucket, Slot slot, SnapshotStorage storage) {
        this.bucket = bucket;
        this.taskId = taskId;
        this.slot = slot;
        this.storage = storage;
    }

    private String generateKey(Slot slot) {
        final String key = "task/" + taskId + "/slot/" + slot.name();
        return key.replace("//", "/");
    }

    @Override
    public URI uri() {
        return storage.getURI(bucket, generateKey(slot));
    }

    private StreamsWrapper createStreams() {
        PipedInputStream is = new PipedInputStream();
        PipedOutputStream os;
        try {
            os = new PipedOutputStream(is);
        } catch (IOException e) {
            try {
                is.close();
            } catch (IOException e1) {
                e.addSuppressed(e1);
            }
            throw new RuntimeException("S3ExecutionSnapshot::createStreams exception while creating streams", e);
        }
        initBucket();
        final ListenableFuture<UploadState> future = storage.transmitter().upload(new UploadRequestBuilder()
                .bucket(bucket)
                .key(generateKey(slot))
                .metadata(Metadata.empty())
                .stream(() -> is)
                .build());
        return new StreamsWrapper(is, os, future);
    }

    private void initStream() {
        try{
            lock.lock();
            if (slotStream == null) {
                slotStream = createStreams();
            }
        }
        finally {
            lock.unlock();
        }
    }

    private void initBucket() {
        if (bucketInited.compareAndSet(false, true)) {
            if (!storage.isBucketExist(bucket)) {
                storage.createBucket(bucket);
            }
        }
    }

    @Override
    public void onChunk(ByteString chunk) {
        LOG.info("S3SlotSnapshot::onChunk invoked with slot " + slot.name());
        initStream();
        slotStream.write(chunk);
        nonEmpty.set(true);
    }

    @Override
    public boolean isEmpty() {
        LOG.info("S3SlotSnapshot::isEmpty invoked with slot " + slot.name());
        return !nonEmpty.get();
    }

    @Override
    public void readAll(InputStream stream) {
        LOG.info("S3SlotSnapshot::readAll invoked with slot " + slot.name());
        initStream();
        slotStream.write(stream);
        nonEmpty.set(true);
        onFinish();
    }

    @Override
    public void onFinish() {
        LOG.info("S3SlotSnapshot::onFinish invoked with slot " + slot.name());
        try {
            lock.lock();
            if (slotStream == null){
                return;
            }
            slotStream.close();
            slotStream = null;
        }
        finally {
            lock.unlock();
        }

    }
}
