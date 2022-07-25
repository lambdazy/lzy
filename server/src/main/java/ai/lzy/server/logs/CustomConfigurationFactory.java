package ai.lzy.server.logs;


import java.util.Objects;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.ConfigurationFactory;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.Order;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import ai.lzy.model.utils.KafkaLogsConfiguration;

@Plugin(name = "CustomConfigurationFactory", category = "ConfigurationFactory")
@Order(10)
public class CustomConfigurationFactory extends ConfigurationFactory {

    public static final String[] SUFFIXES = new String[] {".yaml", ".yml", "*"};

    public Configuration getConfiguration(LoggerContext loggerContext, ConfigurationSource configurationSource) {
        System.out.println("ConfigurationFactory:: Configuring kafka appender");
        return new KafkaLogsConfiguration(
            loggerContext,
            configurationSource,
            Objects.equals(System.getenv("KAFKA_LOGS_ENABLED"), "true"),
            System.getenv("KAFKA_LOGS_HOST"),
            "{"
                + "\"timestamp\":\"%d{yyyy-MM-dd HH:mm:ss.SSS}{UTC}\","
                + " \"thread\": \"%t\","
                + "  \"level\": \"%-5level\","
                + " \"logger\": \"%logger{36}\","
                + " \"message\": \"%enc{%msg}{JSON}\","
                + " \"exception\": \"%enc{%ex}{JSON}\""
                + "}",
            "server"
        );
    }

    public String[] getSupportedTypes() {
        return SUFFIXES;
    }
}

