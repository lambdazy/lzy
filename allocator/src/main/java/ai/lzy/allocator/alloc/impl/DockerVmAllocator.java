package ai.lzy.allocator.alloc.impl;

import ai.lzy.allocator.alloc.VmAllocator;
import ai.lzy.allocator.model.Vm;
import ai.lzy.allocator.model.Workload;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import com.github.dockerjava.core.DockerClientBuilder;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import org.apache.commons.lang.SystemUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

@Singleton
@Requires(property = "allocator.docker-allocator.enabled", value = "true")
public class DockerVmAllocator implements VmAllocator {
    private static final DockerClient DOCKER = DockerClientBuilder.getInstance().build();
    private static final Logger LOG = LogManager.getLogger(DockerVmAllocator.class);

    private String requestAllocation(Workload workload) {

        final HostConfig hostConfig = new HostConfig();

        final var portBindings = workload.portBindings().entrySet().stream()
            .map(e -> new PortBinding(Ports.Binding.bindPort(e.getKey()), ExposedPort.tcp(e.getValue())))
            .toList();

        hostConfig
            .withPrivileged(true)
            .withPortBindings(portBindings)
            .withPublishAllPorts(true);

        if (SystemUtils.IS_OS_LINUX) {
            hostConfig.withNetworkMode("host");
        }

        final var exposedPorts = workload.portBindings().values().stream()
            .map(ExposedPort::tcp)
            .toList();

        final var envs = workload.env().entrySet().stream()
            .map(e -> e.getKey() + "=" + e.getValue())
            .toList();

        final CreateContainerResponse container = DOCKER.createContainerCmd(workload.image())
            .withAttachStdout(true)
            .withAttachStderr(true)
            .withEnv(envs)
            .withExposedPorts(exposedPorts)
            .withHostConfig(hostConfig)
            .withCmd(workload.args())
            .exec();
        LOG.info("Created vm container id = {}", container.getId());
        DOCKER.startContainerCmd(container.getId()).exec();
        return container.getId();
    }

    @Override
    public Map<String, String> allocate(Vm vm) {
        var containerId = requestAllocation(vm.workloads().get(0));  // Support only one workload for now
        return Map.of("container-id", containerId);
    }

    @Override
    public void deallocate(Vm vm) {
        var meta = vm.allocatorMeta();
        if (meta == null) {
            throw new RuntimeException("Allocator metadata is null");
        }
        var containerId = meta.get("container-id");
        if (containerId == null) {
            throw new RuntimeException("Container is not set in metadata");
        }
        DOCKER.killContainerCmd(containerId).exec();
        DOCKER.removeContainerCmd(containerId).exec();
    }
}
