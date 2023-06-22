package ai.lzy.service.operations.stop;

import ai.lzy.longrunning.OperationRunnerBase.StepResult;
import ai.lzy.service.BeanFactory;
import ai.lzy.service.dao.StopExecutionState;
import ai.lzy.service.kafka.KafkaLogsListeners;
import ai.lzy.service.operations.ExecutionStepContext;
import ai.lzy.service.operations.RetryableFailStep;
import ai.lzy.util.kafka.KafkaAdminClient;
import ai.lzy.v1.kafka.KafkaS3Sink;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.util.function.Supplier;

import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.util.grpc.GrpcUtils.withIdempotencyKey;

final class DeleteKafkaTopic extends StopExecutionContextAwareStep implements Supplier<StepResult>, RetryableFailStep {
    private final KafkaAdminClient kafkaClient;
    private final KafkaLogsListeners kafkaLogsListeners;
    private final BeanFactory.S3SinkClient s3SinkClient;

    public DeleteKafkaTopic(ExecutionStepContext stepCtx, StopExecutionState state, KafkaAdminClient kafkaClient,
                            KafkaLogsListeners kafkaLogsListeners, BeanFactory.S3SinkClient s3SinkClient)
    {
        super(stepCtx, state);
        this.kafkaClient = kafkaClient;
        this.kafkaLogsListeners = kafkaLogsListeners;
        this.s3SinkClient = s3SinkClient;
    }

    @Override
    public StepResult get() {
        if (kafkaTopicDesc() == null) {
            log().debug("{} Kafka topic description is null, skip step...", logPrefix());
            return StepResult.ALREADY_DONE;
        }

        log().info("{} Delete kafka topic with name='{}'", logPrefix(), kafkaTopicDesc().topicName());

        kafkaLogsListeners.notifyFinished(execId());

        try {
            kafkaClient.dropUser(kafkaTopicDesc().username());
        } catch (StatusRuntimeException sre) {
            if (sre.getStatus().getCode() == Status.Code.NOT_FOUND) {
                // it may be idempotent call
                log().debug("{} Kafka user with name='{}' not found: {}", logPrefix(), kafkaTopicDesc().username(),
                    sre.getMessage(), sre);
            } else {
                return retryableFail(sre, "Cannot drop kafka user with name='%s'".formatted(
                    kafkaTopicDesc().username()), sre);
            }
        }

        try {
            kafkaClient.dropTopic(kafkaTopicDesc().topicName());
        } catch (StatusRuntimeException sre) {
            if (sre.getStatus().getCode() == Status.Code.NOT_FOUND) {
                // it may be idempotent call
                log().debug("{} Kafka topic with name='{}' not found: {}", logPrefix(), kafkaTopicDesc().topicName(),
                    sre.getMessage(), sre);
            } else {
                return retryableFail(sre, "Cannot drop kafka topic with name='%s'".formatted(
                    kafkaTopicDesc().topicName()), sre);
            }
        }

        if (kafkaTopicDesc().sinkJobId() != null) {
            log().info("{} Stop remote job on s3-sink for topic with name='{}'", logPrefix(),
                kafkaTopicDesc().topicName());

            var s3SinkStub = (idempotencyKey() != null) ?
                withIdempotencyKey(s3SinkClient.stub(), idempotencyKey() + "_stop_sync") : s3SinkClient.stub();

            try {
                //noinspection ResultOfMethodCallIgnored
                s3SinkStub.stop(KafkaS3Sink.StopRequest.newBuilder().setJobId(kafkaTopicDesc().sinkJobId()).build());
            } catch (StatusRuntimeException sre) {
                return retryableFail(sre, "Cannot stop remote job with id='%s' on s3sink".formatted(
                    kafkaTopicDesc().sinkJobId()), sre);
            }
        }

        try {
            withRetries(log(), () -> execDao().setKafkaTopicDesc(execId(), null, null));
        } catch (Exception e) {
            return retryableFail(e, "Error while cleaning kafka topic description in dao", Status.INTERNAL
                .withDescription("Cannot delete kafka topic").asRuntimeException());
        }

        log().debug("{} Kafka topic with name='{}' successfully deleted", logPrefix(), kafkaTopicDesc().topicName());
        setKafkaTopicDesc(null);

        return StepResult.CONTINUE;
    }
}
