package ai.lzy.server.local.allocators;

import ai.lzy.server.Authenticator;
import ai.lzy.server.ServantsAllocatorBase;
import ai.lzy.server.configs.ServerConfig;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.*;
import com.github.dockerjava.core.DockerClientBuilder;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import org.apache.commons.lang.SystemUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ai.lzy.model.graph.Provisioning;
import ai.lzy.model.utils.FreePortFinder;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static ai.lzy.model.Constants.LOGS_DIR;

@Singleton
@Requires(property = "server.dockerAllocator.enabled", value = "true")
public class DockerServantsAllocator extends ServantsAllocatorBase {
    private static final DockerClient DOCKER = DockerClientBuilder.getInstance().build();
    private static final Logger LOG = LogManager.getLogger(DockerServantsAllocator.class);
    private final ServerConfig serverConfig;
    private final Map<String, ContainerDescription> containers = new ConcurrentHashMap<>();

    public DockerServantsAllocator(Authenticator auth, ServerConfig serverConfig) {
        super(auth, 60, 900);
        this.serverConfig = serverConfig;
    }

    @Override
    protected void requestAllocation(String sessionId, String servantId, String servantToken, Provisioning provisioning, String bucket) {
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
                "BASE_ENV_DEFAULT_IMAGE=" + serverConfig.getBaseEnvDefaultImage()
            )
            .withExposedPorts(ExposedPort.tcp(debugPort), ExposedPort.tcp(servantPort), ExposedPort.tcp(fsPort))
            .withHostConfig(hostConfig)
            .withCmd(
                "--lzy-address", serverConfig.getServerUri().toString(),
                "--lzy-whiteboard", serverConfig.getWhiteboardUri().toString(),
                "--lzy-mount", "/tmp/lzy",
                "--host", serverConfig.getServerUri().getHost(),
                "--port", Integer.toString(servantPort),
                "--fs-port", Integer.toString(fsPort),
                "start",
                "--bucket", bucket,
                "--sid", servantId,
                "--token", servantToken
            )
            .exec();
        LOG.info("Created servant container id = {}", container.getId());

        LOG.info("Starting servant container with id={}", container.getId());
        DOCKER.startContainerCmd(container.getId()).exec();

        containers.put(servantId, new ContainerDescription(container));
    }

    @Override
    protected void cleanup(ServantConnection s) {
        if (!containers.containsKey(s.id())) {
            return;
        }
        ContainerDescription container = containers.get(s.id());
        InspectContainerResponse containerInfo = DOCKER.inspectContainerCmd(container.container.getId()).exec();
        if (Boolean.TRUE.equals(containerInfo.getState().getDead())) {
            containers.remove(s.id());
            return;
        }
        terminate(s);
    }

    @Override
    protected void terminate(ServantConnection connection) {
        var container = containers.remove(connection.id());
        if (container != null) {
            container.close();
        }
    }

    private static class ContainerDescription {
        private final CreateContainerResponse container;

        public ContainerDescription(CreateContainerResponse container) {
            this.container = container;
        }

        public void close() {
            DOCKER.killContainerCmd(container.getId()).exec();
            DOCKER.removeContainerCmd(container.getId()).withForce(true).exec();
        }
    }
}
