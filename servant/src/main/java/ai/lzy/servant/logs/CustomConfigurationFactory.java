package ai.lzy.servant.logs;


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

        if (Objects.equals(System.getenv("LOGS_APPENDER"), "Kafka")
                && System.getenv("SERVANT_ID") == null) {
            throw new RuntimeException("SERVANT_ID env is null. Logging configuration failed.");
        }

        System.out.println("ConfigurationFactory:: Configuring kafka appender");

        return new KafkaLogsConfiguration(
            loggerContext,
            configurationSource,
            Objects.equals(System.getenv("LOGS_APPENDER"), "Kafka"),
            System.getenv("LOGS_SERVER"),
            "{"
                + "\"timestamp\":\"%d{yyyy-MM-dd HH:mm:ss.SSS}{UTC}\", "
                + "\"thread\": \"%t\", "
                + "\"level\": \"%-5level\", "
                + "\"logger\": \"%logger{36}\", "
                + "\"message\": \"%enc{%msg}{JSON}\", "
                + "\"servant\": \"" + System.getenv("SERVANT_ID") + "\", "
                + "\"exception\": \"%enc{%ex}{JSON}\""
                + "}",
            "servant"
        );
    }

    public String[] getSupportedTypes() {
        return SUFFIXES;
    }
}

