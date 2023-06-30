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
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;

public class Job {
    private static final Logger LOG = LogManager.getLogger(Job.class);
    private static final int BUFFER_SIZE = 5 << 20; // S3 multipart chunk must be at least 5Mb
    private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

    private final String id;
    private final KafkaS3Sink.StartRequest request;
    private final S3SinkMetrics metrics;
    private final ServiceConfig config;
    private final AtomicReference<Instant> deadline = new AtomicReference<>(null);
    private final Consumer<String, byte[]> consumer;
    private final S3AsyncClient storageClient;  // Support only s3 for now
    @Nullable
    private Iterator<ConsumerRecord<String, byte[]>> resultsStream = null;
    private final Map<String, StreamUploadDesc> streams = new HashMap<>();

    public record JobStatus(
        boolean completed,
        Duration restartAfter
    ) {
        private static JobStatus restartAfter(Duration delay) {
            return new JobStatus(false, delay);
        }

        private static JobStatus continue_() {
            return new JobStatus(false, Duration.ZERO);
        }

        private static JobStatus complete() {
            return new JobStatus(true, Duration.ZERO);
        }
    }

    public Job(String id, KafkaHelper helper, KafkaS3Sink.StartRequest request, S3SinkMetrics metrics,
               ServiceConfig config)
    {
        this.id = id;
        this.request = request;
        this.metrics = metrics;
        this.config = config;

        if (!request.hasS3()) {
            LOG.error("{} Failed to upload data to s3: Supports only s3 storage for now", this);
            throw Status.UNIMPLEMENTED.withDescription("Supports only s3 storage for now").asRuntimeException();
        }

        var accessToken = request.getS3().getAccessToken();
        var secretToken = request.getS3().getSecretToken();
        var endpoint = request.getS3().getEndpoint();

        var props = helper.toProperties();
        props.put("enable.auto.commit", "false");
        props.put("group.id", id);

        consumer = new KafkaConsumer<>(props);

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

        var partition = new TopicPartition(request.getTopicName(), /* partition */ 0);
        consumer.assign(List.of(partition));
        consumer.seek(partition, /* offset */ 0);

        metrics.activeSessions.inc();
    }

    public String id() {
        return id;
    }

    public synchronized JobStatus run() {
        var deadlineTime = deadline.get();
        if (deadlineTime != null) {
            var completed = false;
            if (Instant.now().isBefore(deadlineTime)) {
                completed = streams.values().stream().allMatch(StreamUploadDesc::completed);
            } else {
                var activeStreams = streams.values().stream().filter(s -> !s.completed()).count();
                LOG.info("{} S3 deadline uploading reached. Got {} uncompleted streams out of {}. Terminate...",
                    this, activeStreams, streams.size());
                streams.values().forEach(s -> s.complete("deadline"));
                completed = true;
            }
            if (completed) {
                resultsStream = null;
                safeCall(consumer::close);
                metrics.activeSessions.dec();
                return JobStatus.complete();
            }
        }

        for (var stream : streams.values()) {
            if (stream.uploading()) {
                return JobStatus.restartAfter(config.getUploadPollInterval());
            }
        }

        if (resultsStream == null) {
            // Blocks here. Kafka does not have non-blocking api for now
            var results = consumer.poll(Duration.ofMillis(100));

            if (results.isEmpty()) {
                return JobStatus.restartAfter(config.getKafkaPollInterval());
            }

            resultsStream = results.iterator();
        }

        while (resultsStream.hasNext()) {
            var res = resultsStream.next();

            var taskId = requireNonNull(res.key());
            var header = requireNonNull(res.headers().lastHeader("stream"));
            var streamType = new String(header.value(), StandardCharsets.US_ASCII);
            var stream = streams.computeIfAbsent(taskId + "." + streamType,
                k -> new StreamUploadDesc(new AmazonS3URI(request.getStoragePrefixUri() + "/" + k)));

            if (stream.failed()) {
                LOG.warn("{} drop data from failed stream {}.{}", this, taskId, streamType);
                continue;
            }

            if (stream.completed()) {
                LOG.error("{} got data from completed stream {}.{}, drop it", this, taskId, streamType);
                continue;
            }

            if (stream.uploadId.isBlank()) {
                stream.init(storageClient);
                if (stream.failed()) {
                    continue;
                }
            }

            var eos = res.headers().lastHeader("eos");
            if (eos != null) {
                if (stream.eos) {
                    LOG.error("{} stream {}.{} already completed", this, taskId, streamType);
                    continue;
                }

                stream.eos = true;
                if (stream.buffer.position() > 0) {
                    stream.startUpload();
                }
                continue;
            }

            if (stream.eos) {
                LOG.error("{} got data for already completed stream {}.{}, drop it", this, taskId, streamType);
                continue;
            }

            if (res.value().length > BUFFER_SIZE) {
                metrics.errors.inc();
                LOG.error("{} got too big chunk for stream {}.{}: {}", this, taskId, streamType, res.value().length);
                continue;
            }

            LOG.debug("{} Read {} bytes from stream s3://{}/{}", this, res.value().length, stream.bucket, stream.key);

            assert stream.state == StreamUploadDesc.State.CollectingData;

            var sizeToCopy = Integer.min(stream.buffer.remaining(), res.value().length);
            stream.buffer.put(res.value(), 0, sizeToCopy);

            if (stream.buffer.remaining() == 0) {
                stream.startUpload();
                if (stream.failed()) {
                    continue;
                }

                // We can put all remaining data in buffer because data fits the buffer size
                // (buffer is empty && res.value().length <= BUFFER_SIZE)
                stream.buffer.put(res.value(), sizeToCopy, res.value().length - sizeToCopy);
            }
        }
        resultsStream = null;

        return JobStatus.continue_();
    }

    public void complete() {
        deadline.set(Instant.now().plus(config.getCompleteJobTimeout()));
    }

    @Override
    public String toString() {
        return "(topic: %s, storage: %s)".formatted(request.getTopicName(), request.getStoragePrefixUri());
    }

    @VisibleForTesting
    public Map<String, StreamUploadDesc> streams() {
        return Collections.unmodifiableMap(streams);
    }

    @FunctionalInterface
    private interface Func {
        void run() throws Exception;
    }

    private static void safeCall(Func fn) {
        try {
            fn.run();
        } catch (Exception e) {
            // ignored
        }
    }


    private final class StreamUploadDesc {
        private final String bucket;
        private final String key;
        private String uploadId = "";
        private int partNumber = 1;
        private State state = State.CollectingData;
        private ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
        private List<CompletedPart> completedParts = new ArrayList<>();
        @Nullable
        private CompletableFuture<UploadPartResponse> uploadAwaitable = null;
        @Nullable
        private Status failStatus = null;
        private boolean eos = false;

        enum State {
            CollectingData,
            UploadingData,
            Completed
        }

        StreamUploadDesc(AmazonS3URI uri) {
            this.bucket = uri.getBucket();
            this.key = uri.getKey();
        }

        void init(S3AsyncClient storageClient) {
            if (state == State.Completed) {
                return;
            }

            try {
                var resp = storageClient.createMultipartUpload(
                        CreateMultipartUploadRequest.builder()
                            .bucket(bucket)
                            .key(key)
                            .contentType(MediaType.TEXT_PLAIN)
                            .build())
                    .get();
                uploadId = resp.uploadId();

                LOG.info("{} Init upload {} to s3://{}/{}", Job.this, uploadId, bucket, key);
            } catch (Exception e) {
                LOG.error("{} Error while creating multipart upload for s3://{}/{}", Job.this, bucket, key, e);
                metrics.errors.inc();

                state = State.Completed;
                failStatus = Status.INTERNAL.withDescription("Init failed: " + e.getMessage());
                buffer = EMPTY_BUFFER;
                completedParts.clear();
            }
        }

        boolean completed() {
            return state == State.Completed;
        }

        boolean failed() {
            return state == State.Completed && failStatus != null;
        }

        boolean uploading() {
            if (state != State.UploadingData) {
                return false;
            }

            requireNonNull(uploadAwaitable);

            if (!uploadAwaitable.isDone()) {
                return true;
            }

            try {
                var resp = uploadAwaitable.get();
                completedParts.add(
                    CompletedPart.builder()
                        .partNumber(partNumber)
                        .eTag(resp.eTag())
                        .build());
                LOG.info("{} Complete upload {} part {} to s3://{}/{}", Job.this, uploadId, partNumber, bucket, key);
            } catch (Exception e) {
                LOG.error("{} Cannot upload part {} to upload {} at s3://{}/{}: {}",
                    Job.this, partNumber, uploadId, bucket, key, e.getMessage(), e);
                metrics.errors.inc();
                state = State.Completed;
                failStatus = Status.INTERNAL.withDescription(e.getMessage());
                buffer = EMPTY_BUFFER;
                completedParts.clear();
                return false;
            }

            partNumber++;
            uploadAwaitable = null;

            if (eos) {
                complete(null);
                return false;
            }

            state = State.CollectingData;
            return false;
        }

        void startUpload() {
            if (state != State.CollectingData) {
                throw new RuntimeException("" + state);
            }

            if (uploadAwaitable != null) {
                throw new RuntimeException("already uploading");
            }

            try {
                buffer.flip();

                uploadAwaitable = storageClient.uploadPart(
                    UploadPartRequest.builder()
                        .bucket(bucket)
                        .key(key)
                        .partNumber(partNumber)
                        .uploadId(uploadId)
                        .build(),
                    AsyncRequestBody.fromByteBuffer(buffer));

                LOG.info("{} Start uploading part {} of upload {} to s3://{}/{} of size {}...",
                    Job.this, partNumber, uploadId, bucket, key, buffer.limit());

                state = State.UploadingData;
                metrics.uploadedBytes.inc(buffer.limit());
                buffer.clear();
            } catch (Exception e) {
                LOG.error("{} S3 upload {} part {} to s3://{}/{} failed: {}",
                    Job.this, uploadId, partNumber, bucket, key, e.getMessage(), e);
                state = State.Completed;
                failStatus = Status.INTERNAL.withDescription(e.getMessage());
                buffer = EMPTY_BUFFER;
                completedParts.clear();
                metrics.errors.inc();
            }
        }

        void complete(@Nullable String error) {
            if (state == State.Completed) {
                return;
            }

            if (uploadAwaitable != null) {
                safeCall(() -> uploadAwaitable.cancel(true));
                uploadAwaitable = null;
            }

            LOG.info("{} Complete upload {} of {} parts to s3://{}.{} by reason '{}' ...",
                Job.this, uploadId, partNumber, bucket, key, error);

            try {
                storageClient.completeMultipartUpload(
                    CompleteMultipartUploadRequest.builder()
                        .uploadId(uploadId)
                        .bucket(bucket)
                        .key(key)
                        .multipartUpload(CompletedMultipartUpload.builder()
                            .parts(completedParts)
                            .build())
                        .build())
                    .get();
                state = State.Completed;
                failStatus = error != null ? Status.INTERNAL.withDescription(error) : null;
            } catch (Exception e) {
                LOG.error("{} Cannot complete upload {} to s3://{}/{}: {}",
                    Job.this, uploadId, bucket, key, e.getMessage(), e);
                metrics.errors.inc();
                state = State.Completed;
                failStatus = Status.INTERNAL
                    .withDescription(error != null ? error + ": " + e.getMessage() : e.getMessage());
            }

            buffer = EMPTY_BUFFER;
            completedParts.clear();
        }
    }
}
