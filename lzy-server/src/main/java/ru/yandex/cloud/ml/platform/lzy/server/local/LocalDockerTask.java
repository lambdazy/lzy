package ru.yandex.cloud.ml.platform.lzy.server.local;

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
import ru.yandex.cloud.ml.platform.lzy.whiteboard.WhiteboardMeta;

import java.net.URI;
import java.util.Map;
import java.util.UUID;

public class LocalDockerTask extends LocalTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(LocalDockerTask.class);

    LocalDockerTask(
        String owner,
        UUID tid,
        Zygote workload,
        Map<Slot, String> assignments,
        WhiteboardMeta meta,
        ChannelsManager channels,
        URI serverURI
    ) {
        super(owner, tid, workload, assignments, meta, channels, serverURI);
    }

    @Override
    protected void runServantAndWaitFor(String serverHost, int serverPort, String servantHost, int servantPort, UUID tid, String token) {
        final String updatedServerHost = SystemUtils.IS_OS_LINUX ? serverHost : serverHost.replace("localhost", "host.docker.internal");
        final String internalHost = SystemUtils.IS_OS_LINUX ? "localhost" : "host.docker.internal";
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
            .withEnv("LZYWHITEBOARD", "http://" + internalHost + ":8999")
            .withEnv("BUCKET_NAME", "lzy-bucket")
            .withEnv("ACCESS_KEY", "access-key")
            .withEnv("SECRET_KEY", "secret-key")
            .withEnv("REGION", "ru-central1")
            .withEnv("SERVICE_ENDPOINT", "storage.yandexcloud.net")
            .withEnv("PATH_STYLE_ACCESS_ENABLED", "false")
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
