package ai.lzy.kafka.s3sink;

import ai.lzy.util.kafka.KafkaHelper;
import ai.lzy.v1.kafka.KafkaS3Sink;
import com.amazonaws.services.s3.AmazonS3URI;
import com.google.common.annotations.VisibleForTesting;
import io.grpc.Status;
import io.micronaut.http.MediaType;
import jakarta.annotation.Nullable;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.*;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class Job {
    private static final Logger LOG = LogManager.getLogger(Job.class);
    private static final int BUFFER_SIZE = 1024 * 1024 * 5; // S3 multipart chunk must be at least 5Mb
    public static final AtomicInteger COMPLETE_TIMEOUT_MS = new AtomicInteger(5000);
    public static final AtomicInteger UPLOADING_TIMEOUT_MS = new AtomicInteger(100);
    public static final AtomicInteger KAFKA_POLLING_TIMEOUT_MS = new AtomicInteger(500);

    private final String id;
    private final AtomicReference<State> state = new AtomicReference<>(State.Created);
    private final KafkaS3Sink.StartRequest request;
    private final S3SinkMetrics metrics;
    private final AtomicReference<Instant> deadline = new AtomicReference<>(null);
    private final List<CompletedPart> completedParts = new ArrayList<>();
    private final ByteBuffer s3ChunkBuffer = ByteBuffer.allocate(BUFFER_SIZE);
    private final Consumer<String, byte[]> consumer;
    private final S3AsyncClient storageClient;  // Support only s3 for now
    private final Set<String> activeStreams = ConcurrentHashMap.newKeySet();
    @Nullable
    private String multipartId;
    private int partNumber = 1;
    @Nullable
    private Iterator<ConsumerRecord<String, byte[]>> resultsStream;
    @Nullable
    private CompletableFuture<UploadPartResponse> uploadAwaitable;
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

    public Job(String id, KafkaHelper helper, KafkaS3Sink.StartRequest request, S3SinkMetrics metrics) {
        this.id = id;
        this.request = request;
        this.metrics = metrics;

        if (!request.getStorageConfig().hasS3()) {
            LOG.error("{} Failed to upload data to s3: Supports only s3 storage for now", this);
            throw Status.UNIMPLEMENTED.withDescription("Supports only s3 storage for now").asRuntimeException();
        }

        var accessToken = request.getStorageConfig().getS3().getAccessToken();
        var secretToken = request.getStorageConfig().getS3().getSecretToken();
        var endpoint = request.getStorageConfig().getS3().getEndpoint();

        var props = helper.toProperties();
        props.put("enable.auto.commit", "false");
        props.put("group.id", id);

        consumer = new KafkaConsumer<>(props);
        metrics.activeSessions.inc();

        try {
            storageClient = S3AsyncClient.builder()
                .credentialsProvider(
                    StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(accessToken, secretToken)))
                .region(Region.US_WEST_1)
                .endpointOverride(new URI(endpoint))
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
            resp = storageClient.createMultipartUpload(
                    CreateMultipartUploadRequest.builder()
                        .bucket(bucket)
                        .key(key)
                        .contentType(MediaType.TEXT_PLAIN)
                        .build())
                .get();
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("{} Error while creating multipart upload", this, e);
            metrics.errors.inc();

            try {
                storageClient.abortMultipartUpload(
                        AbortMultipartUploadRequest.builder()
                            .uploadId(multipartId)
                            .bucket(bucket)
                            .key(key)
                            .build())
                    .get();
            } catch (InterruptedException | ExecutionException ex) {
                metrics.errors.inc();
                LOG.error("{} Error while aborting upload", this, ex);
            }

            consumer.close();
            metrics.activeSessions.dec();
            throw Status.INTERNAL.asRuntimeException();
        }

        multipartId = resp.uploadId();

        var partition = new TopicPartition(request.getTopicName(), /* partition */ 0);
        consumer.assign(List.of(partition));
        consumer.seek(partition, /* offset */ 0);
    }

    public String id() {
        return id;
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
                    if (deadline.get() != null && (activeStreams.isEmpty() || Instant.now().isAfter(deadline.get()))) {
                        if (s3ChunkBuffer.position() > 0) {  // If some data remaining in chunk buffer, uploading it
                            s3ChunkBuffer.flip();

                            uploadAwaitable = storageClient.uploadPart(
                                UploadPartRequest.builder()
                                    .bucket(bucket)
                                    .key(key)
                                    .partNumber(partNumber)
                                    .uploadId(multipartId)
                                    .build(),
                                AsyncRequestBody.fromByteBuffer(s3ChunkBuffer));

                            metrics.uploadedBytes.inc(s3ChunkBuffer.limit());
                            s3ChunkBuffer.clear();

                            state.set(State.AwaitingUpload);
                            yield new PollResult(false, null, Duration.ofMillis(UPLOADING_TIMEOUT_MS.get()));
                        }

                        // No more records available, completing job
                        state.set(State.Completed);
                        yield new PollResult(false, null, Duration.ZERO);
                    }

                    // Sleeping for 0.5s to wait for records
                    yield new PollResult(false, null, Duration.ofMillis(KAFKA_POLLING_TIMEOUT_MS.get()));
                }

                resultsStream = results.iterator();
                this.state.set(State.SchedulingUpload);

                yield new PollResult(false, null, Duration.ZERO);
            }

            case SchedulingUpload -> {
                while (true) {
                    try {
                        final var res = resultsStream.next();

                        var taskId = res.key();
                        var header = res.headers().lastHeader("stream");
                        final String streamName;

                        if (header == null) {
                            LOG.warn("{} Cannot get stream name from header", this);
                            streamName = "";
                        } else {
                            streamName = new String(header.value(), StandardCharsets.UTF_8);
                        }

                        if (res.headers().lastHeader("eos") != null) {
                            activeStreams.remove(taskId + "-" + streamName);
                            continue;
                        }

                        activeStreams.add(taskId + "-" + streamName);

                        if (res.value().length > BUFFER_SIZE) {
                            metrics.errors.inc();
                            LOG.error("{} Chunks of size > 5Mb are not supported for now, skipping it", this);
                            continue;
                        }

                        var sizeToCopy = Integer.min(s3ChunkBuffer.remaining(), res.value().length);
                        s3ChunkBuffer.put(res.value(), 0, sizeToCopy);

                        if (s3ChunkBuffer.remaining() == 0) {
                            uploadAwaitable = storageClient.uploadPart(
                                UploadPartRequest.builder()
                                    .bucket(bucket)
                                    .key(key)
                                    .partNumber(partNumber)
                                    .uploadId(multipartId)
                                    .build(),
                                AsyncRequestBody.fromByteBuffer(s3ChunkBuffer));

                            metrics.uploadedBytes.inc(s3ChunkBuffer.limit());
                            s3ChunkBuffer.clear();

                            // We can put all remaining data in buffer because data is no longer then buffer size
                            s3ChunkBuffer.put(res.value(), sizeToCopy, res.value().length - sizeToCopy);

                            state.set(State.AwaitingUpload);
                            yield new PollResult(false, null, Duration.ofMillis(UPLOADING_TIMEOUT_MS.get()));
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
                        metrics.errors.inc();

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

                    storageClient.completeMultipartUpload(
                        CompleteMultipartUploadRequest.builder()
                            .uploadId(multipartId)
                            .bucket(bucket)
                            .key(key)
                            .multipartUpload(completedMultipartUpload)
                            .build())
                        .get();
                } catch (InterruptedException | ExecutionException e) {
                    LOG.error("{} Cannot complete multipart upload: ", this, e);
                    metrics.errors.inc();
                    yield new PollResult(true, Status.INTERNAL, Duration.ZERO);
                }

                metrics.activeSessions.dec();
                consumer.close();

                LOG.info("{} Upload completed: no more chunks available", this);
                yield new PollResult(true, null, Duration.ZERO);
            }
        };
    }

    public void complete() {
        deadline.set(Instant.now().plus(Duration.ofMillis(COMPLETE_TIMEOUT_MS.get())));
    }

    @VisibleForTesting
    public void addActiveStream(String streamName) {
        activeStreams.add(streamName);
    }

    @Override
    public String toString() {
        return "(topic: %s, storage: %s)".formatted(request.getTopicName(), request.getStorageConfig().getUri());
    }
}
