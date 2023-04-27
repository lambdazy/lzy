package ai.lzy.allocator.vmpool;

import jakarta.annotation.Nullable;

import java.util.Map;

public interface VmPoolRegistry {

    Map<String, VmPoolSpec> getSystemVmPools();

    Map<String, VmPoolSpec> getUserVmPools();

    @Nullable
    VmPoolSpec findPool(String poolLabel);

}
