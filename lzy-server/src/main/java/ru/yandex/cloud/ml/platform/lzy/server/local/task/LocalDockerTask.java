package ru.yandex.cloud.ml.platform.lzy.server.local.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.output.WaitingConsumer;
import org.testcontainers.shaded.org.apache.commons.lang.SystemUtils;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.Zygote;
import ru.yandex.cloud.ml.platform.lzy.model.utils.FreePortFinder;
import ru.yandex.cloud.ml.platform.lzy.server.ChannelsManager;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotMeta;
import ru.yandex.qe.s3.util.Environment;

import java.net.URI;
import java.util.Map;
import java.util.UUID;

public class LocalDockerTask extends LocalTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(LocalDockerTask.class);

    public LocalDockerTask(
        String owner,
        UUID tid,
        Zygote workload,
        Map<Slot, String> assignments,
        SnapshotMeta meta,
        ChannelsManager channels,
        URI serverURI
    ) {
        super(owner, tid, workload, assignments, meta, channels, serverURI);
    }

    @Override
    protected void runServantAndWaitFor(String serverHost, int serverPort, String servantHost, int servantPort, UUID tid, String token) {
        final String updatedServerHost = SystemUtils.IS_OS_LINUX ? serverHost : serverHost.replace("localhost", "host.docker.internal");
        final String internalHost = SystemUtils.IS_OS_LINUX ? "localhost" : "host.docker.internal";
        LOGGER.info("Servant s3 service endpoint id " + Environment.getServiceEndpoint());
        final String uuid = UUID.randomUUID().toString().substring(0, 5);
        final int debugPort = FreePortFinder.find(5000, 6000);
        //noinspection deprecation
        final FixedHostPortGenericContainer<?> base = new FixedHostPortGenericContainer<>("lzy-servant")
            .withPrivilegedMode(true) //it is not necessary to use privileged mode for FUSE, but is is easier for testing
            .withEnv("LZYTASK", tid.toString())
            .withEnv("LZYTOKEN", token)
            .withEnv("LOG_FILE", "servant_start_" + uuid)
            .withEnv("DEBUG_PORT", Integer.toString(debugPort))
            .withEnv("SUSPEND_DOCKER", "n")
            //.withFileSystemBind("/var/log/servant/", "/var/log/servant/")
            .withEnv("LZYWHITEBOARD", Environment.getLzyWhiteboard())
            .withEnv("BUCKET_NAME", Environment.getBucketName())
            .withEnv("ACCESS_KEY", Environment.getAccessKey())
            .withEnv("SECRET_KEY", Environment.getSecretKey())
            .withEnv("REGION", Environment.getRegion())
            .withEnv("SERVICE_ENDPOINT", Environment.getServiceEndpoint())
            .withEnv("PATH_STYLE_ACCESS_ENABLED", Environment.getPathStyleAccessEnabled())
            .withEnv("USE_S3_PROXY", String.valueOf(Environment.useS3Proxy()))
            .withEnv("S3_PROXY_PROVIDER", Environment.getS3ProxyProvider())
            .withEnv("S3_PROXY_IDENTITY", Environment.getS3ProxyIdentity())
            .withEnv("S3_PROXY_CREDENTIALS", Environment.getS3ProxyCredentials())
            .withCommand("--lzy-address " + updatedServerHost + ":" + serverPort + " "
                + "--host localhost "
                + "--internal-host " + internalHost + " "
                + "--port " + servantPort + " "
                + "--lzy-mount /tmp/lzy");

        final GenericContainer<?> servantContainer;
        if (SystemUtils.IS_OS_LINUX) {
            servantContainer = base.withNetworkMode("host");
        } else {
            servantContainer = base
                .withFixedExposedPort(servantPort, servantPort)
                .withFixedExposedPort(debugPort, debugPort) //to attach debugger
                .withExposedPorts(servantPort, debugPort);
        }
        servantContainer.start();

        final WaitingConsumer waitingConsumer = new WaitingConsumer();
        servantContainer.followOutput(waitingConsumer);
        servantContainer.followOutput(new Slf4jLogConsumer(LOGGER));
        waitingConsumer.waitUntilEnd();
    }
}
