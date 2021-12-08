package ru.yandex.cloud.ml.platform.lzy.model.utils;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.builder.api.*;
import org.apache.logging.log4j.core.config.builder.impl.BuiltConfiguration;

public class KafkaLogsAppender {
    public static BuiltConfiguration generate(String topic, String pattern, String logsServer){
        ConfigurationBuilder<BuiltConfiguration> builder
            = ConfigurationBuilderFactory.newConfigurationBuilder();
        AppenderComponentBuilder kafka
            = builder.newAppender("Kafka", "Kafka");
        kafka.addAttribute("topic", topic);
        kafka.addAttribute("syncSend", false);
        LayoutComponentBuilder jsonLayout
            = builder.newLayout("PatternLayout");
        jsonLayout.addAttribute("pattern", pattern);
        kafka.add(jsonLayout);
        ComponentBuilder<?> properties = builder.newComponent("Property");
        properties.addAttribute("name", "bootstrap.servers");
        properties.addAttribute("value", logsServer);
        kafka.addComponent(properties);
        builder.add(kafka);
        RootLoggerComponentBuilder rootLogger = builder.newRootLogger(Level.INFO);
        rootLogger.add(builder.newAppenderRef("Kafka"));
        builder.add(rootLogger);
        return builder.build();
    }
}
