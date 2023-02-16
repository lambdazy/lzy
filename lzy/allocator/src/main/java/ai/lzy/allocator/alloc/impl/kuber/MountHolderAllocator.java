package ai.lzy.allocator.alloc.impl.kuber;

import ai.lzy.allocator.exceptions.InvalidConfigurationException;
import ai.lzy.allocator.model.Vm;
import ai.lzy.allocator.model.Workload;

public interface MountHolderAllocator {

    String allocateMountHolder(Vm.Spec vmSpec) throws InvalidConfigurationException;

    void deallocateMountHolder(String podName);

    Workload createRequestTunnelWorkload(String remoteV6, String poolLabel, String zone)
        throws InvalidConfigurationException;
}
