package ai.lzy.allocator.alloc.impl;

import ai.lzy.allocator.alloc.VmAllocator;
import ai.lzy.allocator.alloc.dao.VmDao;
import ai.lzy.allocator.alloc.exceptions.InvalidConfigurationException;
import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.model.Vm;
import ai.lzy.allocator.model.Workload;
import ai.lzy.model.db.TransactionHandle;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.exception.NotFoundException;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import com.github.dockerjava.core.DockerClientBuilder;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.commons.lang3.SystemUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import static ai.lzy.allocator.alloc.impl.ThreadVmAllocator.PORTAL_POOL_LABEL;

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

    private String allocateWithSingleWorkload(String vmId, String vmOtt, Workload workload) {

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
            .collect(Collectors.toList());

        final var envs = workload.env().entrySet().stream()
            .map(e -> e.getKey() + "=" + e.getValue())
            .collect(Collectors.toList());

        final var args = new ArrayList<>(workload.args());

        args.addAll(List.of(
            "--vm-id", vmId,
            "--allocator-address", config.getAddress(),
            "--allocator-heartbeat-period", config.getHeartbeatTimeout().dividedBy(2).toString(),
            "--host", "localhost"
        ));
        args.add("--allocator-token");
        args.add('"' + vmOtt + '"');
        if (workload.env().containsKey("LZY_WORKER_PKEY")) {
            args.add("--iam-token");
            args.add('"' + workload.env().get("LZY_WORKER_PKEY") + '"');
        }

        final CreateContainerResponse container = DOCKER.createContainerCmd(workload.image())
            .withAttachStdout(true)
            .withAttachStderr(true)
            .withEnv(envs)
            .withExposedPorts(exposedPorts)
            .withHostConfig(hostConfig)
            .withCmd(args)
            .exec();
        LOG.info("Created vm {} inside container with id = {}", vmId, container.getId());
        DOCKER.startContainerCmd(container.getId()).exec();
        return container.getId();
    }

    @Override
    public boolean allocate(Vm vm) throws InvalidConfigurationException {
        if (vm.workloads().size() > 1) {
            throw new InvalidConfigurationException("DockerAllocator supports only one workload");
        }
        if (vm.poolLabel().contains(PORTAL_POOL_LABEL)) {
            throw new InvalidConfigurationException("PORTAL_POOL_LABEL is not supported in DockerAllocator");
        }
        var containerId = allocateWithSingleWorkload(vm.vmId(), vm.allocateState().vmOtt(), vm.workloads().get(0));
        try {
            dao.setAllocatorMeta(vm.vmId(), Map.of(CONTAINER_ID_KEY, containerId), null);
            return true;
        } catch (SQLException e) {
            LOG.error("Cannot allocate VM: {}", e.getMessage(), e);
            throw new RuntimeException("Cannot allocate VM: " + e.getMessage(), e);
        }
    }

    @Override
    public void deallocate(String vmId) {
        Map<String, String> meta;
        try {
            meta = dao.getAllocatorMeta(vmId, null);
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
        try {
            DOCKER.killContainerCmd(containerId).exec();
            DOCKER.removeContainerCmd(containerId).exec();
        } catch (NotFoundException e) {
            LOG.info("Container {} for vm {} destroyed before", containerId, vmId, e);
        }
    }

    @Override
    public List<VmEndpoint> getVmEndpoints(String vmId, @Nullable TransactionHandle transaction) {
        final var name = SystemUtils.IS_OS_MAC
            ? "host.docker.internal" : "localhost";  // On mac docker cannot forward ports to localhost
        return List.of(new VmEndpoint(VmEndpointType.HOST_NAME, name));
    }
}
