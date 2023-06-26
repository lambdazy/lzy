package ai.lzy.service.operations.start;

import ai.lzy.longrunning.OperationRunnerBase.StepResult;
import ai.lzy.service.BeanFactory;
import ai.lzy.service.dao.ExecutionDao.KafkaTopicDesc;
import ai.lzy.service.dao.StartExecutionState;
import ai.lzy.service.operations.ExecutionStepContext;
import ai.lzy.service.operations.RetryableFailStep;
import ai.lzy.util.kafka.KafkaAdminClient;
import ai.lzy.v1.common.LMST;
import ai.lzy.v1.kafka.KafkaS3Sink;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.function.Supplier;

import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.util.grpc.GrpcUtils.withIdempotencyKey;

final class CreateKafkaTopic extends StartExecutionContextAwareStep implements Supplier<StepResult>, RetryableFailStep {
    private final KafkaAdminClient kafkaClient;
    private final BeanFactory.S3SinkClient s3SinkClient;
    private final LMST.StorageConfig storageConfig;

    public CreateKafkaTopic(ExecutionStepContext stepCtx, StartExecutionState state, LMST.StorageConfig storageConfig,
                            KafkaAdminClient kafkaClient, BeanFactory.S3SinkClient s3SinkClient)
    {
        super(stepCtx, state);
        this.kafkaClient = kafkaClient;
        this.s3SinkClient = s3SinkClient;
        this.storageConfig = storageConfig;
    }

    @Override
    public StepResult get() {
        if (kafkaTopicDesc() != null) {
            log().debug("{} Kafka topic already created, skip step...", logPrefix());
            return StepResult.ALREADY_DONE;
        }

        var kafkaTopicName = "topic_" + execId() + ".logs";
        var kafkaUsername = "user_" + execId().replace("-", "_");
        var kafkaUserPassword = idGenerator().generate();

        log().info("{} Create kafka topic with name='{}'", logPrefix(), kafkaTopicName);

        try {
            kafkaClient.createTopic(kafkaTopicName);
        } catch (StatusRuntimeException sre) {
            if (sre.getStatus().getCode() == Status.Code.ALREADY_EXISTS) {
                // idempotent call
                log().debug("{} Kafka topic with name='{}' already created", logPrefix(), kafkaTopicName);
            } else {
                return retryableFail(sre, "Cannot create kafka topic with name='%s'".formatted(kafkaTopicName), sre);
            }
        }

        try {
            kafkaClient.createUser(kafkaUsername, kafkaUserPassword);
        } catch (StatusRuntimeException sre) {
            if (sre.getStatus().getCode() == Status.Code.ALREADY_EXISTS) {
                // idempotent call
                log().debug("{} Kafka user with name='{}' already created", logPrefix(), kafkaUsername);
            } else {
                return retryableFail(sre, "Cannot create kafka user with name='%s'".formatted(kafkaUsername), sre);
            }
        }

        final String[] s3sinkJobId = {null};

        try {
            kafkaClient.grantPermission(kafkaUsername, kafkaTopicName);

            var formatter = DateTimeFormatter.ofPattern("dd.MM.yyyy_HH.mm.ss");

            if (s3SinkClient.enabled() && storageConfig.hasS3()) {
                var storagePath = storageConfig.getUri().substring("s3://".length());  // Removing s3 prefix

                var path = Path.of(storagePath)
                    .resolve("logs")
                    .resolve(formatter.format(LocalDateTime.now()) + execId() + ".log");

                var uri = "s3://" + path;

                log().info("{} Starting remote job on s3-sink with topicName='{}', bucketUri='{}'", logPrefix(),
                    kafkaTopicName, storageConfig.getUri());

                var s3SinkStub = (idempotencyKey() != null) ?
                    withIdempotencyKey(s3SinkClient.stub(), idempotencyKey() + "_start_sync") : s3SinkClient.stub();
                var resp = s3SinkStub.start(KafkaS3Sink.StartRequest.newBuilder()
                    .setTopicName(kafkaTopicName)
                    .setStorageConfig(LMST.StorageConfig.newBuilder()
                        .setUri(uri)
                        .setS3(storageConfig.getS3())
                        .build())
                    .build());

                s3sinkJobId[0] = resp.getJobId();
            }

            var topicDesc = new KafkaTopicDesc(kafkaUsername, kafkaUserPassword, kafkaTopicName, s3sinkJobId[0]);

            withRetries(log(), () -> execDao().setKafkaTopicDesc(execId(), topicDesc, null));
            setKafkaTopicDesc(topicDesc);

            log().debug("{} Kafka topic with name='{}' successfully created...", logPrefix(), kafkaTopicName);
        } catch (Exception e) {
            Runnable dropKafka = () -> {
                if (s3sinkJobId[0] != null) {
                    var s3SinkStub = (idempotencyKey() != null) ?
                        withIdempotencyKey(s3SinkClient.stub(), idempotencyKey() + "_stop_sync") : s3SinkClient.stub();

                    try {
                        //noinspection ResultOfMethodCallIgnored
                        s3SinkStub.stop(KafkaS3Sink.StopRequest.newBuilder().setJobId(s3sinkJobId[0]).build());
                    } catch (StatusRuntimeException sre) {
                        log().warn("{} Cannot stop remote job on s3sink after error: {}", logPrefix(), e.getMessage(),
                            sre);
                    }
                }

                try {
                    kafkaClient.dropUser(kafkaUsername);
                } catch (StatusRuntimeException sre) {
                    log().warn("{} Cannot remove kafka user with name='{}' after error {}: ", logPrefix(),
                        kafkaUsername, e.getMessage(), sre);
                }

                try {
                    kafkaClient.dropTopic(kafkaTopicName);
                } catch (StatusRuntimeException sre) {
                    log().warn("{} Cannot remove topic with name='{}' after error {}: ", logPrefix(), kafkaTopicName,
                        e.getMessage(), sre);
                }
            };

            return retryableFail(e, "Cannot create kafka topic with name='%s'".formatted(kafkaTopicName), dropKafka,
                Status.INTERNAL.withDescription("Cannot create kafka topic").asRuntimeException());
        }

        return StepResult.CONTINUE;
    }
}
