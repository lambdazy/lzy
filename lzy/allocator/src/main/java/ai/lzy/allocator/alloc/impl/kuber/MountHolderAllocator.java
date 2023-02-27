package ai.lzy.allocator.alloc.impl.kuber;

import ai.lzy.allocator.exceptions.InvalidConfigurationException;
import ai.lzy.allocator.model.Vm;
import ai.lzy.allocator.model.VolumeClaim;
import ai.lzy.allocator.model.Workload;

public interface MountHolderAllocator {

    String allocateMountHolder(VolumeClaim volumeClaim, Vm.Spec mountToVm) throws InvalidConfigurationException;

    void deallocateMountHolder(String podName);

    Workload createWorkload(String remoteV6, String poolLabel, String zone)
        throws InvalidConfigurationException;
}
