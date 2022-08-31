package ai.lzy.allocator.alloc;

import ai.lzy.allocator.alloc.exceptions.InvalidConfigurationException;
import ai.lzy.allocator.model.Vm;

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
}
