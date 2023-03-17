package ai.lzy.logs;

import jakarta.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

@Getter
@Setter
public final class KafkaConfig {
    private static final Logger LOG = LogManager.getLogger(KafkaConfig.class);

    private boolean enabled = false;
    private List<String> bootstrapServers = new ArrayList<>();
    private String username;
    private String password;
    private String sslCaUrl = null;
    private String sslCaPath = null;
    private String sslKeystorePath = null;
    private String keystorePassword = null;

    public KafkaHelper helper() {
        return new KafkaHelper(this);
    }


    public static final class KafkaHelper {
        private final boolean enabled;
        public static final AtomicBoolean USE_AUTH = new AtomicBoolean(true);

        @Nullable
        private String username;
        @Nullable
        private String password;

        @Nullable
        private List<String> bootstrapServers;

        @Nullable
        private final String keystorePath;
        @Nullable
        private final String keystorePassword;


        public KafkaHelper(KafkaConfig config) {
            if (!config.isEnabled()) {
                this.enabled = false;
                keystorePassword = null;
                keystorePath = null;
                return;
            }
            enabled = true;

            username = config.getUsername();
            password = config.getPassword();
            bootstrapServers = config.getBootstrapServers();

            if (config.getSslKeystorePath() != null) {
                keystorePath = config.getSslKeystorePath();
                keystorePassword = config.getKeystorePassword();
                return;
            }

            if (config.getSslCaPath() != null) {
                LOG.info("Building ssl keystore for kafka helper by pom file...");

                keystorePath = "/tmp/lzy_kafka_keystore" + UUID.randomUUID() + ".jks";
                keystorePassword = "123456";

                try {
                    buildKeystore(config.getSslCaPath(), keystorePath, keystorePassword);
                } catch (IOException | InterruptedException e) {
                    LOG.error("Error while building kafka config", e);
                    throw new RuntimeException("Error while building kafka config", e);
                }
                return;
            }

            if (config.getSslCaUrl() != null) {
                LOG.info("Building ssl keystore for kafka helper by url...");
                var sslCaPath = "/tmp/lzy_kafka_sa" + UUID.randomUUID() + ".pem";

                try {
                    downloadSslCa(config.getSslCaUrl(), sslCaPath);

                    keystorePath = "/tmp/lzy_kafka_keystore" + UUID.randomUUID() + ".jks";
                    keystorePassword = "123456";

                    buildKeystore(sslCaPath, keystorePath, keystorePassword);
                } catch (IOException | InterruptedException e) {
                    LOG.error("Error while building kafka config", e);
                    throw new RuntimeException("Error while building kafka config", e);
                }
                return;
            }

            keystorePath = null;
            keystorePassword = null;
        }

        private void buildKeystore(String sslCaPath, String keystorePath, String keystorePassword)
            throws IOException, InterruptedException
        {
            var proc = Runtime.getRuntime().exec(new String[]{"keytool", "-importcert", "-alias", "YandexCA",
                "-file", sslCaPath,
                "-keystore", keystorePath,
                "-storepass", keystorePassword,
                "--noprompt"});

            var res = proc.waitFor();

            if (res != 0) {
                throw new RuntimeException("""
                    Cannot build keystore from cert:
                        Out: %s
                        Err: %s"""
                    .formatted(
                        new BufferedReader(new InputStreamReader(proc.getInputStream()))
                            .lines().collect(Collectors.joining("\n")),
                        new BufferedReader(new InputStreamReader(proc.getErrorStream()))
                            .lines().collect(Collectors.joining("\n"))
                ));
            }
        }

        private void downloadSslCa(String sslCaUrl, String sslCaPath) throws IOException {
            InputStream in = new URL(sslCaUrl).openStream();
            Files.copy(in, Paths.get(sslCaPath), StandardCopyOption.REPLACE_EXISTING);
        }

        public boolean enabled() {
            return enabled;
        }

        public KafkaHelper withCredentials(List<String> bootstrapServers, String username, String password) {
            this.bootstrapServers = bootstrapServers;
            this.username = username;
            this.password = password;
            return this;
        }

        @Nullable
        public Properties props() {
            if (!enabled) {
                return null;
            }

            var props = new Properties();

            if (username != null && password != null) {
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

            if (bootstrapServers != null) {
                props.put("bootstrap.servers", Strings.join(bootstrapServers, ','));
            }

            if (keystorePassword != null && keystorePath != null) {
                props.put("security.protocol", "SASL_SSL");
                props.put("ssl.truststore.location", keystorePath);
                props.put("ssl.truststore.password", keystorePassword);
            } else {
                props.put("security.protocol", "PLAINTEXT");
            }

            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

            props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

            LOG.debug("Using kafka client with props: {}", props.toString());

            return props;
        }

        public static void setUseAuth(boolean use) {
            USE_AUTH.set(use);
        }
    }
}
