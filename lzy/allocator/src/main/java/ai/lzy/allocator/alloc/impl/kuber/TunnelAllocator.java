package ai.lzy.allocator.alloc.impl.kuber;

import ai.lzy.allocator.alloc.VmAllocator;
import ai.lzy.allocator.exceptions.InvalidConfigurationException;
import ai.lzy.allocator.model.Vm;
import ai.lzy.allocator.model.Workload;

public interface TunnelAllocator {
    /**
     * @return allocated Pod name
     */
    String allocateTunnelAgent(Vm.Spec vmSpec) throws InvalidConfigurationException;

    VmAllocator.Result deleteTunnel(String clusterId, String podName);

    VmAllocator.Result deallocateTunnelAgent(String clusterId, String podName);

    Workload createRequestTunnelWorkload(Vm.TunnelSettings tunnelSettings, String poolLabel, String zone)
        throws InvalidConfigurationException;
}
