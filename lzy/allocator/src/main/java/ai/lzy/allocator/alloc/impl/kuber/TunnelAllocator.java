package ai.lzy.allocator.alloc.impl.kuber;

import ai.lzy.allocator.exceptions.InvalidConfigurationException;
import ai.lzy.allocator.model.Vm;
import ai.lzy.allocator.model.Workload;

public interface TunnelAllocator {
    /**
     * @return allocated Pod name
     */
    String allocateTunnel(Vm.Spec vmSpec) throws InvalidConfigurationException;

    Workload createRequestTunnelWorkload(String remoteV6, String poolLabel, String zone)
        throws InvalidConfigurationException;
}
