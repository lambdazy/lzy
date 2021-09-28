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
import ru.yandex.cloud.ml.platform.lzy.server.ChannelsRepository;

import java.net.URI;
import java.util.Map;
import java.util.UUID;

public class LocalDockerTask extends BaseTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(LocalDockerTask.class);

    LocalDockerTask(
        String owner,
        UUID tid,
        Zygote workload,
        Map<Slot, String> assignments,
        ChannelsRepository channels,
        URI serverURI
    ) {
        super(owner, tid, workload, assignments, channels, serverURI);
    }

    @Override
    protected void runServantAndWaitFor(String serverHost, int serverPort, String servantHost, int servantPort, UUID tid, String token) {
        final String updatedServerHost = SystemUtils.IS_OS_LINUX ? serverHost : serverHost.replace("localhost", "host.docker.internal");
        final String internalHost = SystemUtils.IS_OS_LINUX ? "localhost" : "host.docker.internal";
        final String uuid = UUID.randomUUID().toString().substring(0, 5);
        //noinspection deprecation
        final FixedHostPortGenericContainer<?> base = new FixedHostPortGenericContainer<>("lzy-servant")
            .withPrivilegedMode(true) //it is not necessary to use privileged mode for FUSE, but is is easier for testing
            .withEnv("LZYTASK", tid.toString())
            .withEnv("LZYTOKEN", token)
            .withEnv("LOG_FILE", "servant_start_" + uuid)
            .withEnv("DEBUG_PORT", "5005")
            .withEnv("SUSPEND_DOCKER", "n")
            //.withFileSystemBind("/tmp/lzy/run/", "/var/log/servant/")
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
                .withFixedExposedPort(5005, 5005) //to attach debugger
                .withExposedPorts(servantPort, 5005);
        }
        servantContainer.start();

        final WaitingConsumer waitingConsumer = new WaitingConsumer();
        servantContainer.followOutput(waitingConsumer);
        servantContainer.followOutput(new Slf4jLogConsumer(LOGGER));
        waitingConsumer.waitUntilEnd();
    }
}
