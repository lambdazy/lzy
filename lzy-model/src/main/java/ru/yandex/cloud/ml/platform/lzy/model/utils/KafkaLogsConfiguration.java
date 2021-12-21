package ru.yandex.cloud.ml.platform.lzy.model.utils;

import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.mom.kafka.KafkaAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.config.yaml.YamlConfiguration;
import org.apache.logging.log4j.core.layout.PatternLayout;

import java.util.Objects;

public class KafkaLogsConfiguration extends YamlConfiguration {
    private final boolean kafkaEnabled;
    private final String serverHost;
    private final String pattern;

    public KafkaLogsConfiguration(LoggerContext loggerContext, ConfigurationSource configurationSource, boolean enabled, String host, String pattern) {
        super(loggerContext, configurationSource);
        this.pattern = pattern;
        this.serverHost = host;
        this.kafkaEnabled = enabled;
    }

    @Override
    protected void doConfigure() {
        super.doConfigure();
        if (kafkaEnabled){
            final Configuration config = this;
            final PatternLayout layout = PatternLayout.newBuilder()
                .withPattern(pattern)
                .withConfiguration(config)
                .build();
            final Property[] properties = {Property.createProperty("bootstrap.servers", serverHost)};
            Appender appender = KafkaAppender.newBuilder()
                .setSyncSend(false)
                .setLayout(layout)
                .setTopic("servant")
                .setPropertyArray(properties)
                .setConfiguration(config)
                .setName("Kafka")
                .build();
            appender.start();
            addAppender(appender);
            getRootLogger().addAppender(appender, null, null);
        }
    }
}
