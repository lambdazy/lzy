package ai.lzy.allocator.alloc;

import ai.lzy.allocator.alloc.exceptions.InvalidPoolException;
import ai.lzy.allocator.model.Vm;
import javax.annotation.Nullable;

public interface VmAllocator {
    /**
     * Start vm allocation
     * @param vm vm to allocate
     */
    void allocate(Vm vm) throws InvalidPoolException;

    /**
     * Idempotent operation to destroy vm
     * If vm is not allocated, does nothing
     * @param vm vm to deallocate
     */
    void deallocate(Vm vm);

    /**
     * Get vm description from provisioner
     * @param vm vm to get description of
     */
    @Nullable
    VmDesc getVmDesc(Vm vm);

    enum VmStatus {
        PENDING,
        RUNNING,
        TERMINATED,
        FAILED
    }

    record VmDesc(String sessionId, String name, VmStatus status) {}
}
