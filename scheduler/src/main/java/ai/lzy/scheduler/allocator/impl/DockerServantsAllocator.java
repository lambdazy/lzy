package ai.lzy.scheduler.allocator.impl;

import ai.lzy.model.graph.Provisioning;
import ai.lzy.model.utils.FreePortFinder;
import ai.lzy.scheduler.allocator.ServantMetaStorage;
import ai.lzy.scheduler.allocator.ServantsAllocator;
import ai.lzy.scheduler.configs.ServiceConfig;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.model.*;
import com.github.dockerjava.core.DockerClientBuilder;
import com.google.common.net.HostAndPort;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.commons.lang.SystemUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.UUID;

import static ai.lzy.model.Constants.LOGS_DIR;

@Singleton
@Requires(property = "scheduler.docker-allocator.enabled", value = "true")
public class DockerServantsAllocator implements ServantsAllocator {
    private static final DockerClient DOCKER = DockerClientBuilder.getInstance().build();
    private static final Logger LOG = LogManager.getLogger(DockerServantsAllocator.class);
    private final ServiceConfig config;
    private final ServantMetaStorage metaStorage;

    @Inject
    public DockerServantsAllocator(ServiceConfig config, ServantMetaStorage metaStorage) {
        this.metaStorage = metaStorage;
        this.config = config;
    }

    private String requestAllocation(String workflowId, String servantId, String servantToken) {
        final int debugPort = FreePortFinder.find(5000, 6000);
        LOG.info("Found port for servant {}", debugPort);

        final HostConfig hostConfig = new HostConfig();
        final int servantPort = FreePortFinder.find(10000, 11000);
        final int fsPort = FreePortFinder.find(11000, 12000);
        hostConfig
            .withPrivileged(true)
            .withBinds(
                new Bind(LOGS_DIR + "servant/", new Volume(LOGS_DIR + "servant/")),
                new Bind("/tmp/resources/", new Volume("/tmp/resources/"))
            )
            .withPortBindings(
                new PortBinding(Ports.Binding.bindPort(debugPort), ExposedPort.tcp(debugPort)),
                new PortBinding(Ports.Binding.bindPort(servantPort), ExposedPort.tcp(servantPort)),
                new PortBinding(Ports.Binding.bindPort(fsPort), ExposedPort.tcp(fsPort))
            )
            .withPublishAllPorts(true);

        if (SystemUtils.IS_OS_LINUX) {
            hostConfig.withNetworkMode("host");
        }

        final String uuid = UUID.randomUUID().toString().substring(0, 5);

        final CreateContainerResponse container = DOCKER.createContainerCmd("lzy-servant")
            .withAttachStdout(true)
            .withAttachStderr(true)
            .withEnv(
                "LOG_FILE=" + LOGS_DIR + "servant/servant_start_" + uuid,
                "DEBUG_PORT=" + debugPort,
                "SUSPEND_DOCKER=" + "n",
                "BASE_ENV_DEFAULT_IMAGE=" + config.baseEnvDefaultImage()
            )
            .withExposedPorts(ExposedPort.tcp(debugPort), ExposedPort.tcp(servantPort), ExposedPort.tcp(fsPort))
            .withHostConfig(hostConfig)
            .withCmd(
                "--lzy-address", "http://" + config.schedulerAddress(),
                "--lzy-whiteboard", "http://" + config.whiteboardAddress(),
                "--lzy-mount", "/tmp/lzy",
                "--host", HostAndPort.fromString(config.schedulerAddress()).getHost(),
                "--port", Integer.toString(servantPort),
                "--fs-port", Integer.toString(fsPort),
                "start",
                "--workflow_id", workflowId, // TODO(artolord) add workflow_id to servant
                "--sid", servantId,
                "--token", servantToken
            )  // TODO(artolord) remove bucket from servant
            .exec();
        LOG.info("Created servant container id = {}", container.getId());

        LOG.info("Starting servant container with id={}", container.getId());
        DOCKER.startContainerCmd(container.getId()).exec();

        return container.getId();
    }

    @Override
    public void allocate(String workflowId, String servantId, Provisioning provisioning) {
        final String containerId = requestAllocation(workflowId, servantId, "");  // TODO(artolord) add token generation
        metaStorage.saveMeta(workflowId, servantId, containerId);
    }

    @Override
    public void destroy(String workflowId, String servantId) throws Exception {
        var containerId = metaStorage.getMeta(workflowId, servantId);
        metaStorage.clear(workflowId, servantId);
        if (containerId == null) {
            throw new Exception("Cannot get servant from db");
        }
        DOCKER.killContainerCmd(containerId).exec();
        DOCKER.removeContainerCmd(containerId).exec();
    }
}
