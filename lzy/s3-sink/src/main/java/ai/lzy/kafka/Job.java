package ai.lzy.kafka;

import ai.lzy.util.kafka.KafkaHelper;
import ai.lzy.v1.kafka.KafkaS3Sink;
import com.amazonaws.services.s3.AmazonS3URI;
import com.google.common.net.HostAndPort;
import io.grpc.Status;
import jakarta.annotation.Nullable;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.*;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class Job {
    private static final Logger LOG = LogManager.getLogger(Job.class);
    private static final int BUFFER_SIZE = 1024 * 1024 * 5; // S3 multipart chunk must be at least 5Mb

    private final AtomicReference<State> state = new AtomicReference<>(State.Created);
    private final KafkaS3Sink.StartRequest request;
    private final AtomicBoolean completed = new AtomicBoolean(false);

    private final List<CompletedPart> completedParts = new ArrayList<>();


    private final ByteBuffer s3ChunkBuffer = ByteBuffer.allocate(BUFFER_SIZE);

    @Nullable
    private final Consumer<String, byte[]> consumer;
    @Nullable
    private final S3AsyncClient storageClient;  // Support only s3 for now

    @Nullable
    private String multipartId;
    @Nullable
    private int partNumber = 1;


    @Nullable
    private Iterator<ConsumerRecord<String, byte[]>> resultsStream;

    @Nullable
    CompletableFuture<UploadPartResponse> uploadAwaitable;
    private final String bucket;
    private final String key;

    public record PollResult(
        boolean completed,
        @Nullable Status error,  // Can be not set if not completed
        Duration pollAfter
    ) {}

    enum State {
        Created,
        AwaitingData,
        SchedulingUpload,
        AwaitingUpload,
        Completed
    }

    public Job(KafkaHelper helper, KafkaS3Sink.StartRequest request) {
        this.request = request;

        if (!request.getStorageConfig().hasS3()) {
            LOG.error("{} Failed to upload data to s3: Supports only s3 storage for now", this);

            throw Status.UNIMPLEMENTED.withDescription("Supports only s3 storage for now").asRuntimeException();
        }

        var accessToken = request.getStorageConfig().getS3().getAccessToken();
        var secretToken = request.getStorageConfig().getS3().getSecretToken();
        var endpoint = request.getStorageConfig().getS3().getEndpoint();

        var props = helper.toProperties();
        props.put("enable.auto.commit", "false");
        props.put("group.id", UUID.randomUUID().toString());

        consumer = new KafkaConsumer<>(props);

        var hostAndPort = HostAndPort.fromString(endpoint);

        try {
            storageClient = S3AsyncClient.builder()
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessToken,
                    secretToken)))
                .region(Region.US_WEST_1)
                .endpointOverride(new URI("http", null, hostAndPort.getHost(), hostAndPort.getPort(), null, null, null))
                .forcePathStyle(true)
                .build();
        } catch (URISyntaxException e) {
            LOG.error("{} Provided bad endpoint: {}: ", this, endpoint, e);
            throw Status.INVALID_ARGUMENT.asRuntimeException();
        }

        var uri = new AmazonS3URI(request.getStorageConfig().getUri());

        this.bucket = uri.getBucket();
        this.key = uri.getKey();

        final CreateMultipartUploadResponse resp;
        try {
            resp = storageClient.createMultipartUpload(CreateMultipartUploadRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .contentType("text/plain")
                    .build())
                .get();
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("{} Error while creating multipart upload", this, e);

            try {
                storageClient.abortMultipartUpload(AbortMultipartUploadRequest.builder()
                        .uploadId(multipartId)
                        .bucket(bucket)
                        .key(key)
                        .build())
                    .get();
            } catch (InterruptedException | ExecutionException ex) {
                LOG.error("{} Error while aborting upload", this, ex);
            }

            consumer.close();
            throw Status.INTERNAL.asRuntimeException();
        }

        multipartId = resp.uploadId();

        consumer.assign(List.of(new TopicPartition(request.getTopicName(), 0)));
        consumer.seek(new TopicPartition(request.getTopicName(), 0), 0);
    }


    public synchronized PollResult poll() {
        return switch (state.get()) {
            case Created -> {
                LOG.info("{} Starting upload", this);

                this.state.set(State.AwaitingData);
                yield new PollResult(false, null, Duration.ZERO);
            }

            case AwaitingData -> {

                // Blocks here. Kafka does not have non-blocking api for now
                var results = consumer.poll(Duration.ofMillis(100));

                if (results.isEmpty()) {
                    if (completed.get()) {
                        if (s3ChunkBuffer.position() > 0) {  // If some data remaining in chunk buffer, uploading it
                            var size = s3ChunkBuffer.position();

                            s3ChunkBuffer.position(0);

                            var data = new byte[size];

                            s3ChunkBuffer.get(data);

                            uploadAwaitable = storageClient.uploadPart(UploadPartRequest.builder()
                                    .bucket(bucket)
                                    .key(key)
                                    .partNumber(partNumber)
                                    .uploadId(multipartId)
                                    .build(),
                                AsyncRequestBody.fromBytes(data));

                            s3ChunkBuffer.clear();

                            state.set(State.AwaitingUpload);
                            yield new PollResult(false, null, Duration.ofMillis(100));
                        }

                        // No more records available, completing job
                        state.set(State.Completed);
                        yield new PollResult(false, null, Duration.ZERO);
                    }

                    // Sleeping for 0.5s to wait for records
                    yield new PollResult(false, null, Duration.ofMillis(500));
                }

                resultsStream = results.iterator();
                this.state.set(State.SchedulingUpload);

                yield new PollResult(false, null, Duration.ZERO);
            }

            case SchedulingUpload -> {
                while (true) {
                    try {
                        final var res = resultsStream.next();

                        if (res.value().length > BUFFER_SIZE) {
                            LOG.error("{} Chunks of size > 5Mb are not supported for now, skipping it", this);
                            continue;
                        }

                        var sizeToCopy = Integer.min(s3ChunkBuffer.remaining(), res.value().length);
                        s3ChunkBuffer.put(res.value(), 0, sizeToCopy);

                        if (s3ChunkBuffer.remaining() == 0) {
                            uploadAwaitable = storageClient.uploadPart(UploadPartRequest.builder()
                                    .bucket(bucket)
                                    .key(key)
                                    .partNumber(partNumber)
                                    .uploadId(multipartId)
                                    .build(),
                                AsyncRequestBody.fromByteBuffer(s3ChunkBuffer));

                            s3ChunkBuffer.clear();

                            // We can put all remaining data in buffer because data is no longer then buffer size
                            s3ChunkBuffer.put(res.value(), sizeToCopy, res.value().length - sizeToCopy);

                            state.set(State.AwaitingUpload);
                            yield new PollResult(false, null, Duration.ofMillis(100));
                        }

                    } catch (NoSuchElementException e) {
                        consumer.commitSync();  // Blocks here, but must be not very long
                        this.state.set(State.AwaitingData);
                        yield new PollResult(false, null, Duration.ZERO);
                    }
                }
            }

            case AwaitingUpload -> {
                if (uploadAwaitable.isDone()) {
                    try {
                        var resp = uploadAwaitable.get();
                        completedParts.add(CompletedPart.builder()
                            .partNumber(partNumber)
                            .eTag(resp.eTag())
                            .build());
                    } catch (InterruptedException | ExecutionException e) {
                        LOG.error("{} Cannot upload data to s3: ", this, e);

                        yield new PollResult(true, Status.INTERNAL, Duration.ZERO);
                    }
                    partNumber += 1;
                    uploadAwaitable = null;
                    this.state.set(State.SchedulingUpload);
                    yield new PollResult(false, null, Duration.ZERO);
                }

                yield new PollResult(false, null, Duration.ofMillis(100));
            }

            case Completed -> {
                try {
                    CompletedMultipartUpload completedMultipartUpload = CompletedMultipartUpload.builder()
                        .parts(completedParts)
                        .build();

                    storageClient.completeMultipartUpload(CompleteMultipartUploadRequest.builder()
                        .uploadId(multipartId)
                        .bucket(bucket)
                        .key(key)
                        .multipartUpload(completedMultipartUpload)
                        .build())
                        .get();
                } catch (InterruptedException | ExecutionException e) {

                    LOG.error("{} Cannot complete multipart upload: ", this, e);

                    yield new PollResult(true, Status.INTERNAL, Duration.ZERO);
                }

                consumer.close();

                LOG.info("{} Upload completed: no more chunks available", this);
                yield new PollResult(true, null, Duration.ZERO);
            }
        };
    }

    public void complete() {
        completed.set(true);
    }

    @Override
    public String toString() {
        return "(topic: %s, storage: %s)".formatted(request.getTopicName(), request.getStorageConfig().getUri());
    }
}
