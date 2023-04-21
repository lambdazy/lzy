package ai.lzy.kafka;

import ai.lzy.util.kafka.KafkaHelper;
import ai.lzy.v1.kafka.KafkaS3Sink.CompleteRequest;
import ai.lzy.v1.kafka.KafkaS3Sink.CompleteResponse;
import ai.lzy.v1.kafka.KafkaS3Sink.SubmitRequest;
import ai.lzy.v1.kafka.KafkaS3Sink.SubmitResponse;
import ai.lzy.v1.kafka.S3SinkServiceGrpc;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Named;
import jakarta.inject.Singleton;

@Singleton
public class SinkServiceImpl extends S3SinkServiceGrpc.S3SinkServiceImplBase {
    private final JobExecutor executor;
    private final KafkaHelper helper;

    public SinkServiceImpl(JobExecutor executor, @Named("S3SinkKafkaHelper") KafkaHelper helper) {
        this.executor = executor;
        this.helper = helper;
    }

    @Override
    public void submit(SubmitRequest request, StreamObserver<SubmitResponse> responseObserver) {
        final Job job;
        try {
            job = new Job(helper, request);
        } catch (StatusRuntimeException e) {
            responseObserver.onError(e);
            return;
        }


        var id = executor.submit(job);

        responseObserver.onNext(SubmitResponse.newBuilder()
            .setJobId(id)
            .build());
        responseObserver.onCompleted();
    }

    @Override
    public void complete(CompleteRequest request, StreamObserver<CompleteResponse> responseObserver) {
        executor.complete(request.getJobId());

        responseObserver.onNext(CompleteResponse.newBuilder().build());
        responseObserver.onCompleted();
    }
}
