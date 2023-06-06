package ai.lzy.kafka.s3sink;

import ai.lzy.longrunning.IdempotencyUtils;
import ai.lzy.v1.kafka.KafkaS3Sink.StartRequest;
import ai.lzy.v1.kafka.KafkaS3Sink.StartResponse;
import ai.lzy.v1.kafka.KafkaS3Sink.StopRequest;
import ai.lzy.v1.kafka.KafkaS3Sink.StopResponse;
import ai.lzy.v1.kafka.S3SinkServiceGrpc;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Singleton;

@Singleton
public class SinkServiceImpl extends S3SinkServiceGrpc.S3SinkServiceImplBase {
    private final JobExecutor executor;

    public SinkServiceImpl(JobExecutor executor) {
        this.executor = executor;
    }

    @Override
    public void start(StartRequest request, StreamObserver<StartResponse> responseObserver) {
        final String jobId;

        try {
            jobId = executor.submit(request, IdempotencyUtils.getIdempotencyKey(request));
        } catch (StatusRuntimeException e) {
            responseObserver.onError(e);
            return;
        }

        responseObserver.onNext(StartResponse.newBuilder()
            .setJobId(jobId)
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
