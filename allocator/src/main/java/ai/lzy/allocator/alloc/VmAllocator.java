package ai.lzy.allocator.alloc;

import ai.lzy.allocator.alloc.exceptions.InvalidConfigurationException;
import ai.lzy.allocator.model.Vm;

import java.util.List;

public interface VmAllocator {
    /**
     * Start vm allocation
     *
     * @param vmSpec specify parameters for vm allocation
     */
    void allocate(Vm.Spec vmSpec) throws InvalidConfigurationException;

    /**
     * Idempotent operation to destroy vm
     * If vm is not allocated, does nothing
     *
     * @param vmId of vm to deallocate
     */
    void deallocate(String vmId);

    /**
     * Get hosts of vm to connect to it
     * @param vmId id of vm to get hosts
     * @return List of host of vm
     */
    List<VmHost> vmHosts(String vmId);

    record VmHost(
        String type,  // HostName, ExternalIP or InternalIP, like in k8s node
        String value
    ) {}
}
