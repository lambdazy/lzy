package ai.lzy.allocator.alloc;

import ai.lzy.allocator.model.Vm;
import ai.lzy.model.db.TransactionHandle;

import javax.annotation.Nullable;

public interface VmAllocator {
    /**
     * Start vm allocation
     * @param vm vm to allocate
     */
    void allocate(Vm vm, @Nullable TransactionHandle transaction);

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
        SUCCEEDED,
        FAILED
    }

    record VmDesc(String sessionId, String name, VmStatus status) {}
}
