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

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class S3ExecutionSnapshot implements ExecutionSnapshot {
    private static final Logger LOG = LogManager.getLogger(LzyExecution.class);
    private static final String BUCKET_NAME = System.getenv("BUCKET");
    private static final String ACCESS_KEY = System.getenv("ACCESS_KEY");
    private static final String SECRET_KEY = System.getenv("SECRET_KEY");
    private static final String REGION = "ru-central1";

    private static final String SERVICE_ENDPOINT = "storage.yandexcloud.net";

    private static final Transmitter transmitter;
    static {
        BasicAWSCredentials credentials = new BasicAWSCredentials(ACCESS_KEY, SECRET_KEY);
        AmazonS3 s3 = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withEndpointConfiguration(
                        new AmazonS3ClientBuilder.EndpointConfiguration(
                                SERVICE_ENDPOINT,REGION
                        )
                )
                .build();
        AmazonTransmitterFactory factory = new AmazonTransmitterFactory(s3);
        transmitter = factory.fixedPoolsTransmitter("transmitter", 10, 10);
    }

    private final String taskId;
    private final Map<Slot, StreamsWrapper> slotStream = new ConcurrentHashMap<>();

    public S3ExecutionSnapshot(String taskId) {
        this.taskId = taskId;
    }

    private String generateKey(Slot slot) {
        return "/task/" + taskId + "/slot/" + slot.name();
    }

    @Override
    public URI getSlotUri(Slot slot) {
        try {
            return new URI("https://" + SERVICE_ENDPOINT + "/" + BUCKET_NAME  + generateKey(slot));
        } catch (URISyntaxException e) {
            // never happens
            throw new RuntimeException(e);
        }
    }

    private StreamsWrapper createStreams(Slot slot) {
        PipedInputStream is = new PipedInputStream();
        PipedOutputStream os = null;
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
        final ListenableFuture<UploadState> future = transmitter.upload(new UploadRequestBuilder()
                .bucket(BUCKET_NAME)
                .key(generateKey(slot))
                .metadata(Metadata.empty())
                .stream(() -> is)
                .build());
        return new StreamsWrapper(is, os, future);
    }

    @Override
    public void onChunkInput(ByteString chunk, Slot slot) {
        LOG.info("S3ExecutionSnapshot::onChunkInput invoked with slot " + slot.name());
        slotStream.computeIfAbsent(slot, this::createStreams);
        slotStream.get(slot).write(chunk);
    }

    @Override
    public void onChunkOutput(ByteString chunk, Slot slot) {
        LOG.info("S3ExecutionSnapshot::onChunkOutput invoked with slot " + slot.name());
        slotStream.computeIfAbsent(slot, this::createStreams);
        slotStream.get(slot).write(chunk);
    }

    @Override
    public void onFinish(Slot slot) {
        LOG.info("S3ExecutionSnapshot::onFinish invoked with slot " + slot.name());
        slotStream.computeIfPresent(slot, (k, v) -> {
            v.close();
            return null;
        });
    }
}
