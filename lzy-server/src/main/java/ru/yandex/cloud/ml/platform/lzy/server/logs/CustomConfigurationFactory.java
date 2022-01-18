package ru.yandex.cloud.ml.platform.lzy.server.logs;


import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.ConfigurationFactory;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.Order;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import ru.yandex.cloud.ml.platform.lzy.model.utils.KafkaLogsConfiguration;

import java.util.Objects;

@Plugin(name = "CustomConfigurationFactory", category = "ConfigurationFactory")
@Order(10)
public class CustomConfigurationFactory extends ConfigurationFactory {

    public static final String[] SUFFIXES = new String[] {".yaml", ".yml", "*"};

    public Configuration getConfiguration(LoggerContext loggerContext, ConfigurationSource configurationSource) {
        return new KafkaLogsConfiguration(
            loggerContext,
            configurationSource,
            Objects.equals(System.getenv("KAFKA_LOGS_ENABLED"), "true"),
            System.getenv("KAFKA_LOGS_HOST"),
            "{" +
                    "\"timestamp\":\"%d{UNIX}\"," +
                    " \"thread\": \"%t\"," +
                    "  \"level\": \"%-5level\"," +
                    " \"logger\": \"%logger{36}\"," +
                    " \"message\": \"%enc{%msg}{JSON}\"," +
                    " \"exception\": \"%enc{%ex}{JSON}\"" +
                    "}",
            "server"
        );
    }

    public String[] getSupportedTypes() {
        return SUFFIXES;
    }
}

