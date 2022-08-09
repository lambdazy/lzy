package ai.lzy.allocator.alloc;

import ai.lzy.allocator.model.Vm;

import java.util.Map;

public interface VmAllocator {
    /**
     * Start vm allocation
     * @param vm vm to allocate
     * @return allocator metadata
     */
    Map<String, String> allocate(Vm vm);

    /**
     * Idempotent operation to destroy vm
     * If vm is not allocated, does nothing
     * @param vm vm to deallocate
     */
    void deallocate(Vm vm);
}
