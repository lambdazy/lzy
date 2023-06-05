package ai.lzy.kafka.s3sink;

import ai.lzy.common.IdGenerator;
import ai.lzy.common.RandomIdGenerator;
import ai.lzy.longrunning.IdempotencyUtils;
import ai.lzy.util.kafka.KafkaHelper;
import ai.lzy.v1.kafka.KafkaS3Sink.StartRequest;
import ai.lzy.v1.kafka.KafkaS3Sink.StartResponse;
import ai.lzy.v1.kafka.KafkaS3Sink.StopRequest;
import ai.lzy.v1.kafka.KafkaS3Sink.StopResponse;
import ai.lzy.v1.kafka.S3SinkServiceGrpc;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.micronaut.core.util.clhm.ConcurrentLinkedHashMap;
import jakarta.inject.Named;
import jakarta.inject.Singleton;

import java.util.Map;

@Singleton
public class SinkServiceImpl extends S3SinkServiceGrpc.S3SinkServiceImplBase {
    private final JobExecutor executor;
    private final KafkaHelper helper;
    private final S3SinkMetrics metrics;
    private final IdGenerator idGenerator = new RandomIdGenerator();
    private final Map<String, String> idempotencyKeyToJobId = new ConcurrentLinkedHashMap.Builder<String, String>()
        .maximumWeightedCapacity(1000)  // LRU cache to not overflow memory
        .build();

    public SinkServiceImpl(JobExecutor executor, @Named("S3SinkKafkaHelper") KafkaHelper helper,
                           S3SinkMetrics metrics)
    {
        this.executor = executor;
        this.helper = helper;
        this.metrics = metrics;
    }

    @Override
    public void start(StartRequest request, StreamObserver<StartResponse> responseObserver) {
        var idempotencyKey = IdempotencyUtils.getIdempotencyKey(request);

        if (idempotencyKey != null) {
            var jobId = idempotencyKeyToJobId.get(idempotencyKey.token());

            if (jobId != null) {
                responseObserver.onNext(StartResponse.newBuilder()
                    .setJobId(jobId)
                    .build());
                responseObserver.onCompleted();
                return;
            }
        }

        final Job job;
        try {
            job = new Job(idGenerator.generate("s3sink-"), helper, request, metrics);
        } catch (StatusRuntimeException e) {
            responseObserver.onError(e);
            return;
        }


        executor.submit(job);

        if (idempotencyKey != null) {
            idempotencyKeyToJobId.put(idempotencyKey.token(), job.id());
        }

        responseObserver.onNext(StartResponse.newBuilder()
            .setJobId(job.id())
            .build());
        responseObserver.onCompleted();
    }

    @Override
    public void stop(StopRequest request, StreamObserver<StopResponse> responseObserver) {
        executor.complete(request.getJobId());

        responseObserver.onNext(StopResponse.newBuilder().build());
        responseObserver.onCompleted();
    }
}
