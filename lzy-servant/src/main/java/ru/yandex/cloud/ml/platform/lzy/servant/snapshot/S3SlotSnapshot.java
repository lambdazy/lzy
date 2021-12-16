package ru.yandex.cloud.ml.platform.lzy.servant.snapshot;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import com.amazonaws.auth.BasicAWSCredentials;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.LzyExecution;
import ru.yandex.qe.s3.amazon.transfer.AmazonTransmitterFactory;
import ru.yandex.qe.s3.transfer.Transmitter;
import ru.yandex.qe.s3.transfer.meta.Metadata;
import ru.yandex.qe.s3.transfer.upload.UploadRequestBuilder;
import ru.yandex.qe.s3.transfer.upload.UploadState;
import ru.yandex.qe.s3.util.Environment;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class S3SlotSnapshot implements SlotSnapshot {
    private static final Logger LOG = LogManager.getLogger(LzyExecution.class);
    private final AmazonS3 client;
    private final Transmitter transmitter;

    private final String taskId;
    private final String bucket;
    private final Slot slot;
    private final Map<Slot, StreamsWrapper> slotStream = new ConcurrentHashMap<>();
    private final Set<Slot> nonEmpty = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean bucketInited = new AtomicBoolean(false);

    public S3SlotSnapshot(String taskId, String bucket, Slot slot, Transmitter transmitter, AmazonS3 client) {
        this.bucket = bucket;
        this.taskId = taskId;
        this.slot = slot;
        this.transmitter = transmitter;
        this.client = client;
    }

    private String generateKey(Slot slot) {
        final String key = "task/" + taskId + "/slot/" + slot.name();
        return key.replace("//", "/");
    }

    @Override
    public URI uri() {
        try {
            initBucket();
            return client.getUrl(bucket, generateKey(slot)).toURI();
        } catch (URISyntaxException e) {
            // never happens
            throw new RuntimeException(e);
        }
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
        final ListenableFuture<UploadState> future = transmitter.upload(new UploadRequestBuilder()
                .bucket(bucket)
                .key(generateKey(slot))
                .metadata(Metadata.empty())
                .stream(() -> is)
                .build());
        return new StreamsWrapper(is, os, future);
    }

    private void initBucket() {
        if (bucketInited.compareAndSet(false, true)) {
            if (!client.doesBucketExistV2(bucket)) {
                client.createBucket(bucket);
            }
        }
    }

    @Override
    public void onChunk(ByteString chunk) {
        LOG.info("S3ExecutionSnapshot::onChunk invoked with slot " + slot.name());
        slotStream.computeIfAbsent(slot, slot -> createStreams());
        slotStream.get(slot).write(chunk);
        nonEmpty.add(slot);
    }

    @Override
    public boolean isEmpty() {
        LOG.info("S3ExecutionSnapshot::isEmpty invoked with slot " + slot.name());
        return !nonEmpty.contains(slot);
    }

    @Override
    public void onFinish() {
        LOG.info("S3ExecutionSnapshot::onFinish invoked with slot " + slot.name());
        slotStream.computeIfPresent(slot, (k, v) -> {
            v.close();
            return null;
        });
    }
}
