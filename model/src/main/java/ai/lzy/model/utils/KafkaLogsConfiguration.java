package ai.lzy.model.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.mom.kafka.KafkaAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.config.yaml.YamlConfiguration;
import org.apache.logging.log4j.core.layout.PatternLayout;

public class KafkaLogsConfiguration extends YamlConfiguration {
    private static final Logger LOG = LogManager.getLogger(KafkaLogsConfiguration.class);

    private final boolean kafkaEnabled;
    private final String serverHost;
    private final String pattern;
    private final String topic;

    public KafkaLogsConfiguration(LoggerContext loggerContext, ConfigurationSource configurationSource, boolean enabled,
                                  String host, String pattern, String topic) {
        super(loggerContext, configurationSource);
        this.pattern = pattern;
        this.serverHost = host;
        this.kafkaEnabled = enabled;
        this.topic = topic;

        LOG.info("KafkaLogsConfiguration::constructor "
                           + "pattern=" + pattern + ", "
                           + "serverHost=" + host + ", "
                           + "kafkaEnabled=" + enabled + ", "
                           + "topic=" + topic);
    }

    @Override
    protected void doConfigure() {
        super.doConfigure();
        if (kafkaEnabled) {
            final Configuration config = this;
            final PatternLayout layout = PatternLayout.newBuilder()
                .withPattern(pattern)
                .withConfiguration(config)
                .build();
            final Property[] properties = {Property.createProperty("bootstrap.servers", serverHost)};
            try {
                Appender appender = KafkaAppender.newBuilder()
                    .setLayout(layout)
                    .setTopic(topic)
                    .setPropertyArray(properties)
                    .setConfiguration(config)
                    .setName("Kafka")
                    .build();
                appender.start();
                addAppender(appender);
                getRootLogger().addAppender(appender, null, null);
            } catch (NullPointerException e) {
                // This catch is to avoid this bug in log4j: https://issues.apache.org/jira/browse/LOG4J2-2375
                // Without it server will fall with exception.
                // You must call LoggerContext().reconfigure() after this exception to enable kafka logs.
                LOG.error("NPE while configuring kafka logs");
            } catch (Exception e) {
                LOG.error("Exception while configuring kafka logs");
            }
        }
    }
}
