package ai.lzy.worker;

import ai.lzy.env.logs.LogWriter;
import ai.lzy.util.kafka.KafkaHelper;
import ai.lzy.v1.common.LMO;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class KafkaLogsWriter implements LogWriter {
    private final String taskId;
    private final KafkaProducer<String, byte[]> kafkaClient;
    private final String topic;
    private final Logger logger;

    public KafkaLogsWriter(LMO.KafkaTopicDescription topic, Logger log, String taskId, KafkaHelper helper) {
        this.taskId = taskId;
        var props = helper.toProperties(topic.getUsername(), topic.getPassword());

        this.kafkaClient = new KafkaProducer<>(props);
        this.topic = topic.getTopic();
        this.logger = log;
    }

    @Override
    public void writeLines(String streamName, byte[] lines) throws IOException {
        // Using single partition to manage global order of logs !!!
        try {
            var headers = new RecordHeaders();
            headers.add("stream", streamName.getBytes(StandardCharsets.UTF_8));

            kafkaClient.send(new ProducerRecord<>(topic, /* partition */ 0, taskId, lines, headers)).get();
        } catch (Exception e) {
            logger.warn("Cannot send data to kafka: ", e);
        }
    }

    @Override
    public void writeEos(String streamName) throws IOException {
        try {
            var headers = new RecordHeaders();
            headers.add("stream", streamName.getBytes(StandardCharsets.UTF_8));
            headers.add("eos", new byte[0]);

            kafkaClient.send(new ProducerRecord<>(topic, /* partition */ 0, taskId, new byte[0], headers)).get();
        } catch (Exception e) {
            logger.warn("Cannot send data to kafka: ", e);
        }
    }
}
