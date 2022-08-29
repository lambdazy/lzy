package ai.lzy.allocator.alloc.impl;

import ai.lzy.allocator.AllocatorAgent;
import ai.lzy.allocator.alloc.VmAllocator;
import ai.lzy.allocator.alloc.exceptions.InvalidConfigurationException;
import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.dao.VmDao;
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
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.commons.lang.SystemUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

@Singleton
@Requires(property = "allocator.docker-allocator.enabled", value = "true")
public class DockerVmAllocator implements VmAllocator {

    private static final String CONTAINER_ID_KEY = "container-id";
    private static final DockerClient DOCKER = DockerClientBuilder.getInstance().build();
    private static final Logger LOG = LogManager.getLogger(DockerVmAllocator.class);

    private final VmDao dao;
    private final ServiceConfig config;

    @Inject
    public DockerVmAllocator(VmDao dao, ServiceConfig config) {
        this.dao = dao;
        this.config = config;
    }

    private String requestAllocation(Workload workload, String vmId) {

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

        envs.addAll(List.of(
            AllocatorAgent.VM_ALLOCATOR_ADDRESS + "=" + config.getAddress(),
            AllocatorAgent.VM_HEARTBEAT_PERIOD + "=" + config.getHeartbeatTimeout().dividedBy(2).toString(),
            AllocatorAgent.VM_ID_KEY + "=" + vmId
        ));

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
    public void allocate(Vm vm) throws InvalidConfigurationException {
        if (vm.workloads().size() > 1) {
            throw new InvalidConfigurationException("Docker allocator supports only one workload");
        }
        var containerId = requestAllocation(vm.workloads().get(0), vm.vmId());
        try {
            dao.saveAllocatorMeta(vm.vmId(), Map.of(CONTAINER_ID_KEY, containerId), null);
        } catch (SQLException e) {
            LOG.error("Cannot allocate VM: {}", e.getMessage(), e);
            throw new RuntimeException("Cannot allocate VM: " + e.getMessage(), e);
        }
    }

    @Override
    public void deallocate(Vm vm) {
        Map<String, String> meta;
        try {
            meta = dao.getAllocatorMeta(vm.vmId(), null);
        } catch (SQLException e) {
            throw new RuntimeException("Database error: " + e.getMessage(), e);
        }

        if (meta == null) {
            throw new RuntimeException("Allocator metadata is null");
        }
        var containerId = meta.get(CONTAINER_ID_KEY);
        if (containerId == null) {
            throw new RuntimeException("Container is not set in metadata");
        }
        DOCKER.killContainerCmd(containerId).exec();
        DOCKER.removeContainerCmd(containerId).exec();
    }
}
