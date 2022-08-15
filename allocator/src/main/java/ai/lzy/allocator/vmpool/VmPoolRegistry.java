package ai.lzy.allocator.vmpool;

import com.google.common.net.HostAndPort;

import javax.annotation.Nullable;
import java.util.Map;

public interface VmPoolRegistry {

    Map<String, VmPoolSpec> getSystemVmPools();

    Map<String, VmPoolSpec> getUserVmPools();

    @Nullable
    ClusterCredentials clusterToAllocateVm(String poolLabel, String zone);
    ClusterCredentials getCredential(String clusterId);

    record ClusterCredentials(
        String clusterId,
        HostAndPort masterAddress,
        String masterCert
    ) {}
    
}
