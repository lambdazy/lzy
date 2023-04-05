package ai.lzy.util.kafka;

import org.apache.logging.log4j.util.Strings;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaHelper {
    public static final AtomicBoolean USE_AUTH = new AtomicBoolean(true);

    private final KafkaConfig config;

    public KafkaHelper(KafkaConfig config) {
        assert config.isEnabled();
        this.config = config;
    }

    public Properties toProperties() {
        var props = new Properties();
        props.put("bootstrap.servers", Strings.join(config.getBootstrapServers(), ','));

        if (config.isTlsEnabled()) {
            // encrypt
            props.put("security.protocol", "SASL_SSL");
            props.put("ssl.truststore.location", config.getTlsTruststorePath());
            props.put("ssl.truststore.password", config.getTlsTruststorePassword());

            props.put("ssl.endpoint.identification.algorithm", "");  // Disable endpoint identification
        } else {
            props.put("security.protocol", "PLAINTEXT");
        }

        // auth
        if (config.getScramUsername() != null) {
            fillAuth(props, config.getScramUsername(), config.getScramPassword());
        }

        // serializers
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        return props;

    }

    public Properties toProperties(String username, String password) {
        var props = toProperties();
        fillAuth(props, username, password);
        return props;
    }

    private static void fillAuth(Properties props, String username, String password) {
        if (USE_AUTH.get()) {
            var jaasCfg = "org.apache.kafka.common.security.scram.ScramLoginModule" +
                " required username=\"%s\" password=\"%s\";".formatted(username, password);

            props.put("sasl.jaas.config", jaasCfg);
            props.put("sasl.mechanism", "SCRAM-SHA-512");

        } else {
            var jaasCfg = "org.apache.kafka.common.security.plain.PlainLoginModule" +
                " required username=\"%s\" password=\"%s\";".formatted(username, password);

            props.put("sasl.jaas.config", jaasCfg);
        }
    }
}
