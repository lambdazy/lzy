package ru.yandex.cloud.ml.platform.lzy.servant.logs;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.builder.api.*;
import org.apache.logging.log4j.core.config.builder.impl.BuiltConfiguration;

public class KafkaLogsAppender {
    public static void generate(){
        ConfigurationBuilder<BuiltConfiguration> builder
                = ConfigurationBuilderFactory.newConfigurationBuilder();
        AppenderComponentBuilder kafka
                = builder.newAppender("Kafka", "Kafka");
        kafka.addAttribute("topic", "servant");
        kafka.addAttribute("syncSend", false);
        LayoutComponentBuilder jsonLayout
                = builder.newLayout("PatternLayout");
        jsonLayout.addAttribute("pattern", "{\"timestamp\":\"%d{UNIX}\", \"thread\": \"%t\",  \"level\": \"%-5level\", \"logger\": \"%logger{36}\", \"message\": \"%enc{%msg}{JSON}\", \"servant\": \"" + System.getenv("LZYTASK") + "\"}");
        kafka.add(jsonLayout);
        ComponentBuilder<?> properties = builder.newComponent("Property");
        properties.addAttribute("name", "bootstrap.servers");
        properties.addAttribute("value", System.getenv("LOGS_SERVER"));
        kafka.addComponent(properties);
        builder.add(kafka);
        RootLoggerComponentBuilder rootLogger = builder.newRootLogger(Level.INFO);
        rootLogger.add(builder.newAppenderRef("Kafka"));
        builder.add(rootLogger);
        Configurator.initialize(builder.build());
    }
}
