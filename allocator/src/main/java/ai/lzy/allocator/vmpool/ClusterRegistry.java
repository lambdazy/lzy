package ai.lzy.allocator.vmpool;

import com.google.common.net.HostAndPort;

import javax.annotation.Nullable;

public interface ClusterRegistry {
    @Nullable
    ClusterDescription clusterToAllocateVm(String poolLabel, String zone);

    ClusterDescription getCluster(String clusterId);

    record ClusterDescription(
            String clusterId,
            HostAndPort masterAddress,
            String masterCert
    ) {
    }
}
