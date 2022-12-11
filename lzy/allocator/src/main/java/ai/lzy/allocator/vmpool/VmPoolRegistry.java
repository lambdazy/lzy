package ai.lzy.allocator.vmpool;

import java.util.Map;

public interface VmPoolRegistry {

    Map<String, VmPoolSpec> getSystemVmPools();

    Map<String, VmPoolSpec> getUserVmPools();

}
