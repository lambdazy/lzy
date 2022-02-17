package ru.yandex.cloud.ml.platform.lzy.server.local.task;

import java.net.URI;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.output.WaitingConsumer;
import org.testcontainers.shaded.org.apache.commons.lang.SystemUtils;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.Zygote;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotMeta;
import ru.yandex.cloud.ml.platform.lzy.model.utils.FreePortFinder;
import ru.yandex.cloud.ml.platform.lzy.server.ChannelsManager;

public class LocalDockerTask extends LocalTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(LocalDockerTask.class);

    public LocalDockerTask(
        String owner,
        UUID tid,
        Zygote workload,
        Map<Slot, String> assignments,
        SnapshotMeta meta,
        ChannelsManager channels,
        URI serverURI,
        String bucket
    ) {
        super(owner, tid, workload, assignments, meta, channels, serverURI, bucket);
    }

    @Override
    protected void runServantAndWaitFor(String serverHost, int serverPort, String servantHost, int servantPort,
        UUID tid, String token) {
        final String updatedServerHost =
            SystemUtils.IS_OS_LINUX ? serverHost : serverHost.replace("localhost", "host.docker.internal");
        final String internalHost = SystemUtils.IS_OS_LINUX ? "localhost" : "host.docker.internal";
        LOGGER.info("Servant s3 service endpoint id " + System.getenv("SERVICE_ENDPOINT"));
        final String uuid = UUID.randomUUID().toString().substring(0, 5);
        final int debugPort = FreePortFinder.find(5000, 6000);
        //noinspection deprecation
        final FixedHostPortGenericContainer<?> base = new FixedHostPortGenericContainer<>("lzy-servant")
            .withPrivilegedMode(
                true) //it is not necessary to use privileged mode for FUSE, but is is easier for testing
            .withEnv("LZYTASK", tid.toString())
            .withEnv("LZYTOKEN", token)
            .withEnv("LOG_FILE", "/var/log/servant/servant_start_" + uuid)
            .withEnv("DEBUG_PORT", Integer.toString(debugPort))
            .withEnv("SUSPEND_DOCKER", "n")
            .withEnv("BUCKET_NAME", bucket())
            //.withFileSystemBind("/var/log/servant/", "/var/log/servant/")
            .withEnv("LZYWHITEBOARD", System.getenv("LZYWHITEBOARD"))
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
