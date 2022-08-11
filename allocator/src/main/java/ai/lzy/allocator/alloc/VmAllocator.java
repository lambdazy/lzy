package ai.lzy.allocator.alloc;

import ai.lzy.allocator.model.Vm;
import ai.lzy.model.db.TransactionManager;
import ai.lzy.model.db.TransactionManager.TransactionHandle;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Map;

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
     * Validate that vm is really running after register
     * @param vm vm to validate
     * @return is vm valid?
     */
    boolean validateRunning(Vm vm);
}
