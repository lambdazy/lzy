package ru.yandex.cloud.ml.platform.lzy.iam;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;

public class LzyIAM {

    public static final Logger LOG;

    static{
        // This is to avoid this bug: https://issues.apache.org/jira/browse/LOG4J2-2375
        // KafkaLogsConfiguration will fall, so then we must call reconfigure
        ProducerConfig.configNames();
        LoggerContext ctx = (LoggerContext) LogManager.getContext();
        ctx.reconfigure();
        LOG = LogManager.getLogger(LzyIAM.class);
    }

    public static void main(String[] args) {

    }
}
