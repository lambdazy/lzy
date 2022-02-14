package ru.yandex.cloud.ml.platform.lzy.server.local.task;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.async.ResultCallbackTemplate;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.command.ExecCreateCmd;
import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.command.InspectContainerResponse.ContainerState;
import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.BindOptions;
import com.github.dockerjava.api.model.BindPropagation;
import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.Frame;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.Mount;
import com.github.dockerjava.api.model.MountType;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports.Binding;
import com.github.dockerjava.api.model.Volume;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.command.ExecStartResultCallback;
import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.val;
import org.apache.commons.lang3.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.Zygote;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotMeta;
import ru.yandex.cloud.ml.platform.lzy.model.utils.FreePortFinder;
import ru.yandex.cloud.ml.platform.lzy.server.ChannelsManager;

public class LocalDockerTask extends LocalTask {

    private static final Logger LOG = LoggerFactory.getLogger(LocalDockerTask.class);
    private static final DockerClient DOCKER = DockerClientBuilder.getInstance().build();

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
    protected void runServantAndWaitFor(
        String serverHost,
        int serverPort,
        String servantHost,
        int servantPort,
        UUID tid,
        String token
    ) {
        final String updatedServerHost = SystemUtils.IS_OS_LINUX ? serverHost
            : serverHost.replace("localhost", "host.docker.internal");
        final String internalHost = SystemUtils.IS_OS_LINUX ? "localhost" : "host.docker.internal";
        LOG.info("Servant s3 service endpoint id " + System.getenv("SERVICE_ENDPOINT"));
        final String uuid = UUID.randomUUID().toString().substring(0, 5);
        final int debugPort = FreePortFinder.find(5000, 6000);
        LOG.info("Found port for servant {}", debugPort);

        final HostConfig hostConfig = new HostConfig();
        hostConfig
            .withPrivileged(true)
            .withBinds(
                new Bind("/tmp/resources/", new Volume("/tmp/resources/")),
                new Bind("/tmp/log/servant/", new Volume("/tmp/log/servant/"))
            )
            .withPortBindings(
                new PortBinding(Binding.bindPort(debugPort), ExposedPort.tcp(debugPort)),
                new PortBinding(Binding.bindPort(servantPort), ExposedPort.tcp(servantPort)))
            .withPublishAllPorts(true);

        if (SystemUtils.IS_OS_LINUX) {
            hostConfig.withNetworkMode("host");
        }

        final CreateContainerResponse container = DOCKER.createContainerCmd("lzy-servant")
            .withAttachStdout(true)
            .withAttachStderr(true)
            .withEnv(
                "LZYTASK=" + tid.toString(),
                "LZYTOKEN=" + token,
                "LOG_FILE=" + "/tmp/log/servant/servant_start_" + uuid,
                "DEBUG_PORT=" + Integer.toString(debugPort),
                "SUSPEND_DOCKER=" + "n",
                "LZYWHITEBOARD=" + System.getenv("LZYWHITEBOARD"),
                "BUCKET_NAME=" + bucket(),
                "ACCESS_KEY=" + System.getenv("ACCESS_KEY"),
                "SECRET_KEY=" + System.getenv("SECRET_KEY"),
                "REGION=" + System.getenv("REGION"),
                "SERVICE_ENDPOINT=" + System.getenv("SERVICE_ENDPOINT"),
                "PATH_STYLE_ACCESS_ENABLED=" + System.getenv("PATH_STYLE_ACCESS_ENABLED"),
                "USE_S3_PROXY=" + String.valueOf(Objects.equals(System.getenv("USE_S3_PROXY"), "true")),
                "S3_PROXY_PROVIDER=" + System.getenv("S3_PROXY_PROVIDER"),
                "S3_PROXY_IDENTITY=" + System.getenv("S3_PROXY_IDENTITY"),
                "S3_PROXY_CREDENTIALS=" + System.getenv("S3_PROXY_CREDENTIALS")
            )
            .withExposedPorts(ExposedPort.tcp(debugPort), ExposedPort.tcp(servantPort))
            .withHostConfig(hostConfig)
            .withCmd(("--lzy-address " + updatedServerHost + ":" + serverPort + " "
                + "--host localhost "
                + "--internal-host " + internalHost + " "
                + "--port " + servantPort + " "
                + "--lzy-mount /tmp/lzy").split(" "))
            .exec();
        LOG.info("Created servant container id = {}", container.getId());

        final var attach = DOCKER.attachContainerCmd(container.getId())
            .withFollowStream(true).withStdErr(true).withStdOut(true)
            .exec(new ResultCallbackTemplate<>() {
                @Override
                public void onNext(Frame item) {
                    switch (item.getStreamType()) {
                        case STDOUT:
                            LOG.info(new String(item.getPayload(), StandardCharsets.UTF_8));
                            break;
                        case STDERR:
                            LOG.error(new String(item.getPayload(), StandardCharsets.UTF_8));
                            break;
                        default:
                            LOG.info("Got frame " +
                                new String(item.getPayload(), StandardCharsets.UTF_8) +
                                " from unknown stream type " +
                                item.getStreamType());
                    }
                }
            });

        LOG.info("Starting servant container with id = " + container.getId());
        DOCKER.startContainerCmd(container.getId()).exec();
        LOG.info("Started servant container with id = " + container.getId());

        try {
            attach.awaitCompletion();
        } catch (InterruptedException e) {
            LOG.error("Servant container with id=" + container.getId() + " was interrupted");
        }
        // TODO (lindvv): Check if container is alive
        final InspectContainerResponse inspectContainerResponse =
            DOCKER.inspectContainerCmd(container.getId()).exec();
        final ContainerState state = inspectContainerResponse.getState();
        if (state != null && state.getRunning() != null && state.getRunning()) {
            DOCKER.killContainerCmd(container.getId()).exec();
        }
    }
}
