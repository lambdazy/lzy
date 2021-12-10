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
    private static final String BUCKET_NAME = Environment.getBucketName();
    private static final String ACCESS_KEY = Environment.getAccessKey();
    private static final String SECRET_KEY = Environment.getSecretKey();
    private static final String REGION = Environment.getRegion();
    private static final String SERVICE_ENDPOINT = Environment.getServiceEndpoint();
    private static final String PATH_STYLE_ACCESS_ENABLED = Environment.getPathStyleAccessEnabled();

    private static final Transmitter transmitter;
    private static final AmazonS3 client;

    static {
        BasicAWSCredentials credentials = new BasicAWSCredentials(ACCESS_KEY, SECRET_KEY);
        client = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withEndpointConfiguration(
                        new AmazonS3ClientBuilder.EndpointConfiguration(
                                SERVICE_ENDPOINT, REGION
                        )
                )
                .withPathStyleAccessEnabled(Boolean.parseBoolean(PATH_STYLE_ACCESS_ENABLED))
                .build();
        AmazonTransmitterFactory factory = new AmazonTransmitterFactory(client);
        transmitter = factory.fixedPoolsTransmitter("transmitter", 10, 10);
    }

    private final String taskId;
    private final Slot slot;
    private final Map<Slot, StreamsWrapper> slotStream = new ConcurrentHashMap<>();
    private final Set<Slot> nonEmpty = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean bucketInited = new AtomicBoolean(false);

    public S3SlotSnapshot(String taskId, Slot slot) {
        this.taskId = taskId;
        this.slot = slot;
    }

    private String generateKey(Slot slot) {
        return "task" + taskId + "/slot" + slot.name();
    }

    @Override
    public URI uri() {
        try {
            initBucket();
            return client.getUrl(BUCKET_NAME, generateKey(slot)).toURI();
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
                .bucket(BUCKET_NAME)
                .key(generateKey(slot))
                .metadata(Metadata.empty())
                .stream(() -> is)
                .build());
        return new StreamsWrapper(is, os, future);
    }

    private void initBucket() {
        if (bucketInited.compareAndSet(false, true)) {
            if (!client.doesBucketExistV2(BUCKET_NAME)) {
                client.createBucket(BUCKET_NAME);
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
