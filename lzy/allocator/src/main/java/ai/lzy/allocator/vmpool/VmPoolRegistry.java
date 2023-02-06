package ai.lzy.allocator.vmpool;

import java.util.Map;
import javax.annotation.Nullable;

public interface VmPoolRegistry {

    Map<String, VmPoolSpec> getSystemVmPools();

    Map<String, VmPoolSpec> getUserVmPools();

    @Nullable
    VmPoolSpec findPool(String poolLabel);

}
