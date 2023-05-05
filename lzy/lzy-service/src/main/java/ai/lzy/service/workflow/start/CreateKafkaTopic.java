package ai.lzy.service.workflow.start;

import ai.lzy.longrunning.OperationRunnerBase.StepResult;
import ai.lzy.service.data.KafkaTopicDesc;
import ai.lzy.service.data.dao.ExecutionDao;
import ai.lzy.util.kafka.KafkaAdminClient;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.apache.logging.log4j.Logger;

import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;

import static ai.lzy.model.db.DbHelper.withRetries;

public class CreateKafkaTopic implements Supplier<StepResult> {
    private final ExecutionDao execDao;
    private final String execId;
    private final KafkaAdminClient kafkaClient;
    private final Function<StatusRuntimeException, StepResult> failAction;
    private final Logger log;
    private final String logPrefix;

    public CreateKafkaTopic(ExecutionDao execDao, String execId, KafkaAdminClient kafkaClient,
                            Function<StatusRuntimeException, StepResult> failAction, Logger log, String logPrefix)
    {
        this.execDao = execDao;
        this.execId = execId;
        this.kafkaClient = kafkaClient;
        this.failAction = failAction;
        this.log = log;
        this.logPrefix = logPrefix;
    }

    @Override
    public StepResult get() {
        var kafkaTopicName = "topic_" + execId + ".logs";
        var kafkaUsername = "user_" + execId.replace("-", "_");
        var kafkaUserPassword = UUID.randomUUID().toString();

        log.info("{} Create kafka topic: { execId: {}, topicName: {} }", logPrefix, execId, kafkaTopicName);

        try {
            kafkaClient.createTopic(kafkaTopicName);
            kafkaClient.createUser(kafkaUsername, kafkaUserPassword);
            kafkaClient.grantPermission(kafkaUsername, kafkaTopicName);

            var topicDesc = new KafkaTopicDesc(kafkaUsername, kafkaUserPassword, kafkaTopicName);
            withRetries(log, () -> execDao.setKafkaTopicDesc(execId, topicDesc, null));
        } catch (Exception e) {
            log.error("{} Cannot create kafka topic: {}", logPrefix, e.getMessage(), e);

            try {
                kafkaClient.dropUser(kafkaUsername);
            } catch (StatusRuntimeException sre) {
                log.warn("{} Cannot remove kafka user after error {}: ", logPrefix, e.getMessage(), sre);
            }

            try {
                kafkaClient.dropTopic(kafkaTopicName);
            } catch (StatusRuntimeException sre) {
                log.warn("{} Cannot remove topic after error {}: ", logPrefix, e.getMessage(), sre);
            }

            return (e instanceof StatusRuntimeException) ? failAction.apply((StatusRuntimeException) e)
                : failAction.apply(Status.INTERNAL.withDescription("Cannot create kafka topic").asRuntimeException());
        }

        return StepResult.CONTINUE;
    }
}
