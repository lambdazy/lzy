package ai.lzy.allocator.util;

import ai.lzy.allocator.alloc.impl.kuber.KuberVmAllocator;
import ai.lzy.allocator.model.Vm;
import jakarta.annotation.Nullable;

public final class AllocatorUtils {
    private AllocatorUtils() { }

    @Nullable
    public static String getClusterId(Vm vm) {
        var meta = vm.allocateState().allocatorMeta();
        if (meta == null) {
            return null;
        }

        return meta.get(KuberVmAllocator.CLUSTER_ID_KEY);
    }

}
