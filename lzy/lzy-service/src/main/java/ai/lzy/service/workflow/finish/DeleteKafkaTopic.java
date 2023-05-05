package ai.lzy.service.workflow.finish;

import ai.lzy.longrunning.OperationRunnerBase.StepResult;
import ai.lzy.service.data.KafkaTopicDesc;
import ai.lzy.service.data.dao.ExecutionDao;
import ai.lzy.service.kafka.KafkaLogsListeners;
import ai.lzy.util.kafka.KafkaAdminClient;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.apache.logging.log4j.Logger;

import java.util.function.Function;
import java.util.function.Supplier;

import static ai.lzy.model.db.DbHelper.withRetries;

public class DeleteKafkaTopic implements Supplier<StepResult> {
    private final ExecutionDao execDao;
    private final String execId;
    private final KafkaAdminClient kafkaClient;
    private final KafkaLogsListeners kafkaLogsListeners;
    private final Function<StatusRuntimeException, StepResult> failAction;
    private final Logger log;
    private final String logPrefix;

    public DeleteKafkaTopic(ExecutionDao execDao, String execId,
                            KafkaAdminClient kafkaClient,
                            KafkaLogsListeners kafkaLogsListeners,
                            Function<StatusRuntimeException, StepResult> failAction,
                            Logger log, String logPrefix)
    {
        this.execDao = execDao;
        this.execId = execId;
        this.kafkaClient = kafkaClient;
        this.kafkaLogsListeners = kafkaLogsListeners;
        this.failAction = failAction;
        this.log = log;
        this.logPrefix = logPrefix;
    }

    @Override
    public StepResult get() {
        KafkaTopicDesc kafkaDesc = null;
        try {
            kafkaDesc = withRetries(log, () -> execDao.getKafkaTopicDesc(execId, null));
        } catch (Exception e) {
            log.error("{} Error while obtaining kafka topic description, please clear it: {}", logPrefix,
                e.getMessage(), e);
            failAction.apply(Status.INTERNAL.withDescription("Cannot delete kafka topic").asRuntimeException());
        }

        if (kafkaDesc == null) {
            log.debug("{} Kafka description is null, skip step...", logPrefix);
            return StepResult.CONTINUE;
        }

        log.debug("{} Delete kafka topic and user: { topicName: {}, kafkaUser: {} }", logPrefix, kafkaDesc.topicName(),
            kafkaDesc.username());

        kafkaLogsListeners.notifyFinished(execId);

        try {
            kafkaClient.dropUser(kafkaDesc.username());
        } catch (StatusRuntimeException sre) {
            if (sre.getStatus().getCode() != Status.Code.NOT_FOUND) {
                log.error("{} Cannot remove kafka user: {}", logPrefix, sre.getMessage(), sre);
                return failAction.apply(sre);
            }
            log.warn("{} Kafka user with name='{}' not found: {}", logPrefix, kafkaDesc.username(), sre.getMessage(),
                sre);
        }

        try {
            kafkaClient.dropTopic(kafkaDesc.topicName());
        } catch (StatusRuntimeException sre) {
            if (sre.getStatus().getCode() != Status.Code.NOT_FOUND) {
                log.error("{} Cannot remove kafka topic: {}", logPrefix, sre.getMessage(), sre);
                return failAction.apply(sre);
            }
            log.warn("{} Kafka topic with name='{}' not found: {}", logPrefix, kafkaDesc.topicName(), sre.getMessage(),
                sre);
        }

        try {
            withRetries(log, () -> execDao.setKafkaTopicDesc(execId, null, null));
        } catch (Exception e) {
            log.error("{} Error while cleaning kafka topic description in dao: {}", logPrefix, e.getMessage(), e);
            return failAction.apply(Status.INTERNAL.withDescription("Cannot delete kafka topic").asRuntimeException());
        }

        return StepResult.CONTINUE;
    }
}
