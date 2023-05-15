package ai.lzy.worker;

import ai.lzy.allocator.AllocatorAgent;
import ai.lzy.fs.LzyFsServer;
import ai.lzy.model.utils.FreePortFinder;
import ai.lzy.util.auth.credentials.RsaUtils;
import ai.lzy.util.kafka.KafkaConfig;
import io.grpc.Server;
import io.micronaut.context.ApplicationContext;
import io.micronaut.runtime.Micronaut;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

@Singleton
public class Worker {
    private static final Logger LOG = LogManager.getLogger(Worker.class);
    private static final int DEFAULT_FS_PORT = 9876;
    private static final int DEFAULT_API_PORT = 9877;
    private static final int DEFAULT_HTTP_PORT = 9878;
    private static final AtomicBoolean SELECT_RANDOM_VALUES = new AtomicBoolean(false);

    private static final Options options = new Options();

    static {
        options.addOption(null, "allocator-address", true, "Lzy allocator address [host:port]");
        options.addOption("ch", "channel-manager", true, "Channel manager address [host:port]");
        options.addOption("i", "iam", true, "Iam address [host:port]");
        options.addOption("h", "host", true, "Worker and FS host name");
        options.addOption(null, "vm-id", true, "Vm id from allocator");
        options.addOption(null, "allocator-heartbeat-period", true, "Allocator heartbeat period in duration format");
        options.addOption(null, "kafka-bootstrap", true, "Kafka bootstrap servers");
        options.addOption(null, "truststore-base64", true, "base64 encoded truststore file for kafka logs");
        options.addOption(null, "truststore-password", true, "Truststore password for kafka logs");

        // for tests only
        options.addOption(null, "allocator-token", true, "OTT token for allocator");
    }

    private final LzyFsServer lzyFs;
    private final AllocatorAgent allocatorAgent;
    private final Server server;
    private final ApplicationContext context;

    public Worker(ApplicationContext context, ServiceConfig config, @Named("WorkerServer") Server server,
                  LzyFsServer lzyFsServer, @Named("AllocatorAgent") AllocatorAgent allocatorAgent)
    {
        this.context = context;
        this.server = server;
        this.lzyFs = lzyFsServer;
        this.allocatorAgent = allocatorAgent;

        LOG.info("Starting worker on vm {}.\n apiPort: {}\n fsPort: {}\n host: {}", config.getVmId(),
            config.getApiPort(), config.getFsPort(), config.getHost());

        try {
            server.start();
        } catch (IOException e) {
            LOG.error(e);
            throw new RuntimeException(e);
        }

        try {
            lzyFs.start();
        } catch (IOException e) {
            LOG.error("Error while building uri", e);
            server.shutdown();
            throw new RuntimeException(e);
        }

        try {
            allocatorAgent.start(Map.of(
                MetadataConstants.PUBLIC_KEY, config.getPublicKey(),
                MetadataConstants.API_PORT, String.valueOf(config.getApiPort()),
                MetadataConstants.FS_PORT, String.valueOf(config.getFsPort())
            ));
        } catch (AllocatorAgent.RegisterException e) {
            throw new RuntimeException(e);
        }
        LOG.info("Worker inited");
    }

    public void stop() {
        LOG.error("Stopping worker");
        server.shutdown();
        allocatorAgent.shutdown();
        lzyFs.stop();
        context.stop();
    }

    private void awaitTermination() throws InterruptedException {
        server.awaitTermination();
    }

    public static int execute(String[] args) {
        LOG.info("Starting worker with args [{}]", Arrays.stream(args)
            .map(s -> s.startsWith("-----BEGIN RSA PRIVATE KEY-----") ? "<rsa-private-key>" : s)
            .collect(Collectors.joining(", ")));

        final CommandLineParser cliParser = new DefaultParser();
        try {
            final CommandLine parse = cliParser.parse(options, args, true);

            var allocHeartbeat = parse.getOptionValue("allocator-heartbeat-period");

            allocHeartbeat = allocHeartbeat == null
                ? System.getenv(AllocatorAgent.VM_HEARTBEAT_PERIOD)
                : allocHeartbeat;

            var vmId = parse.getOptionValue("vm-id");
            vmId = vmId == null ? System.getenv(AllocatorAgent.VM_ID_KEY) : vmId;

            var allocatorAddress = parse.getOptionValue("allocator-address");

            allocatorAddress = allocatorAddress == null
                ? System.getenv(AllocatorAgent.VM_ALLOCATOR_ADDRESS)
                : allocatorAddress;

            var allocHeartbeatDur = allocHeartbeat == null ? null : Duration.parse(allocHeartbeat);
            var iamAddress = parse.getOptionValue("iam");
            var channelManagerAddress = parse.getOptionValue("channel-manager");
            var host = parse.getOptionValue('h');
            host = host == null ? System.getenv(AllocatorAgent.VM_IP_ADDRESS) : host;

            var allocatorToken = parse.getOptionValue("allocator-token");
            allocatorToken = allocatorToken == null ? System.getenv(AllocatorAgent.VM_ALLOCATOR_OTT) : allocatorToken;

            var gpuCountStr = System.getenv(AllocatorAgent.VM_GPU_COUNT);
            var gpuCount = gpuCountStr == null ? 0 : Integer.parseInt(gpuCountStr);

            allocatorToken = allocatorToken == null
                ? System.getenv(AllocatorAgent.VM_ALLOCATOR_OTT)
                : allocatorToken;

            var kafkaConf = new KafkaConfig();
            kafkaConf.setBootstrapServers(Arrays.stream(parse.getOptionValue("kafka-bootstrap").split(",")).toList());

            var truststore = parse.getOptionValue("truststore-base64");
            if (truststore != null) {
                kafkaConf.setTlsEnabled(true);
                var path = "/tmp/lzy_" + UUID.randomUUID() + ".jks";

                try (var file = new FileOutputStream(path)) {
                    file.write(Base64.getDecoder().decode(truststore));
                }

                kafkaConf.setTlsTruststorePath(path);
                kafkaConf.setTlsTruststorePassword(parse.getOptionValue("truststore-password"));
            }

            var ctx = startApplication(vmId, allocatorAddress, iamAddress, allocHeartbeatDur,
                channelManagerAddress, host, allocatorToken, gpuCount, kafkaConf);
            var worker = ctx.getBean(Worker.class);

            try {
                worker.awaitTermination();
            } catch (InterruptedException e) {
                worker.stop();
            }
            return 0;
        } catch (ParseException e) {
            LOG.error("Cannot correctly parse options", e);
            return -1;
        } catch (Exception e) {
            LOG.error("Error while executing: " + String.join(" ", args), e);
            return -1;
        } finally {
            try {
                Thread.sleep(100);  // Sleep some time to wait for all logs to be written
            } catch (InterruptedException e) {
                // ignored
            }
        }
    }

    public static ApplicationContext startApplication(String vmId, String allocatorAddress, String iamAddress,
                                                      Duration allocatorHeartbeatPeriod,
                                                      String channelManagerAddress, String host, String allocatorToken,
                                                      int gpuCount, KafkaConfig kafka)
    {
        final int fsPort;
        final String fsRoot;
        final int apiPort;
        final int httpPort;

        if (SELECT_RANDOM_VALUES.get()) {
            fsPort = FreePortFinder.find(10000, 20000);
            apiPort = FreePortFinder.find(20000, 30000);
            httpPort = FreePortFinder.find(30000, 40000);
            fsRoot = "/tmp/lzy" + UUID.randomUUID();
        } else {
            fsPort = DEFAULT_FS_PORT;
            apiPort = DEFAULT_API_PORT;
            httpPort = DEFAULT_HTTP_PORT;
            fsRoot = "/tmp/lzy";
        }

        final RsaUtils.RsaKeys iamKeys;
        try {
            iamKeys = RsaUtils.generateRsaKeys();
        } catch (IOException | InterruptedException e) {
            LOG.error("Cannot generate keys");
            throw new RuntimeException(e);
        }

        var properties = new HashMap<String, Object>(Map.of(
            "worker.vm-id", vmId,
            "worker.allocator-address", allocatorAddress,
            "worker.channel-manager-address", channelManagerAddress,
            "worker.host", host,
            "worker.allocator-token", allocatorToken,
            "worker.fs-port", fsPort,
            "worker.api-port", apiPort,
            "worker.mount-point", fsRoot,
            "worker.iam.address", iamAddress,
            "worker.iam.internal-user-name", vmId
        ));

        properties.put("worker.iam.internal-user-private-key", iamKeys.privateKey());
        properties.put("worker.public-key", iamKeys.publicKey());
        properties.put("worker.allocator-heartbeat-period", allocatorHeartbeatPeriod);
        properties.put("worker.gpu-count", gpuCount);

        properties.put("worker.enable-http-debug", true);

        properties.put("worker.kafka.bootstrap-servers", String.join(",", kafka.getBootstrapServers()));

        if (kafka.isTlsEnabled()) {
            properties.put("worker.kafka.tls-enabled", true);
            properties.put("worker.kafka.tls-truststore-path", kafka.getTlsTruststorePath());
            properties.put("worker.kafka.tls-truststore-password", kafka.getTlsTruststorePassword());
        }

        properties.put("micronaut.server.host", "127.0.0.1"); // ! management api on localhost only !
        properties.put("micronaut.server.port", httpPort);
        properties.put("micronaut.server.netty.access-logger.enabled", "true");

        return Micronaut.build(new String[]{}).properties(properties).start();
    }

    public static void selectRandomValues(boolean val) {
        SELECT_RANDOM_VALUES.set(val);
    }

    public static void main(String[] args) {
        System.exit(execute(args));
    }
}
