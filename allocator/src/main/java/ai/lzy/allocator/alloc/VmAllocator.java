package ai.lzy.allocator.alloc;

import ai.lzy.allocator.alloc.exceptions.InvalidConfigurationException;
import ai.lzy.allocator.model.Vm;

public interface VmAllocator {
    /**
     * Start vm allocation
     *
     * @param vm vm to allocate
     */
    void allocate(Vm vm) throws InvalidConfigurationException;

    /**
     * Idempotent operation to destroy vm
     * If vm is not allocated, does nothing
     *
     * @param vm vm to deallocate
     */
    void deallocate(Vm vm);

    enum VmStatus {
        PENDING,
        RUNNING,
        TERMINATED,
        FAILED
    }

    record VmDesc(String sessionId, String name, VmStatus status) {
    }
}
