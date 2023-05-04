package ai.lzy.util.kafka;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaHelper {
    private static final Logger LOG = LogManager.getLogger(KafkaHelper.class);

    public static final AtomicBoolean USE_AUTH = new AtomicBoolean(true);

    private final KafkaConfig config;

    public KafkaHelper(KafkaConfig config) {
        this.config = config;
    }

    public Properties toProperties() {
        var props = new Properties();
        props.put("bootstrap.servers", Strings.join(config.getBootstrapServers(), ','));

        if (config.isTlsEnabled()) {
            // encrypt
            props.put("ssl.truststore.location", config.getTlsTruststorePath());
            props.put("ssl.truststore.password", config.getTlsTruststorePassword());
            props.put("ssl.truststore.type", config.getTlsTruststoreType());

            if (config.getTlsKeystorePath() != null) {  // tls certs for mTLS auth
                LOG.info("Using ssl keystore auth");

                props.put("ssl.keystore.location", config.getTlsKeystorePath());
                props.put("ssl.keystore.password", config.getTlsKeystorePassword());
                props.put("ssl.keystore.type", config.getTlsKeystoreType());
                props.put("ssl.key.password", config.getTlsKeystorePassword());
            }

            props.put("ssl.endpoint.identification.algorithm", "");  // Disable endpoint identification

            if (config.getScramUsername() != null) {
                props.put("security.protocol", "SASL_SSL");
            } else {
                props.put("security.protocol", "SSL");
            }
        } else {

            if (config.getScramUsername() != null) {
                props.put("security.protocol", "SASL_PLAINTEXT");
            } else {
                props.put("security.protocol", "PLAINTEXT");
            }
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

        props.put("request.timeout.ms", "60000");
        props.put("default.api.timeout.ms", "60000");

        return props;

    }

    public Properties toProperties(String username, String password) {
        var props = toProperties();
        fillAuth(props, username, password);
        return props;
    }

    private void fillAuth(Properties props, String username, String password) {
        if (USE_AUTH.get()) {
            var jaasCfg = "org.apache.kafka.common.security.scram.ScramLoginModule" +
                " required username=\"%s\" password=\"%s\";".formatted(username, password);

            props.put("sasl.jaas.config", jaasCfg);
            props.put("sasl.mechanism", "SCRAM-SHA-512");

            if (config.isTlsEnabled()) {
                props.put("security.protocol", "SASL_SSL");
            } else {
                props.put("security.protocol", "SASL_PLAINTEXT");
            }

        } else {
            var jaasCfg = "org.apache.kafka.common.security.plain.PlainLoginModule" +
                " required username=\"%s\" password=\"%s\";".formatted(username, password);

            props.put("sasl.jaas.config", jaasCfg);
        }
    }
}
