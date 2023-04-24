package ai.lzy.allocator.vmpool;

import com.google.common.net.HostAndPort;
import jakarta.annotation.Nullable;

public interface ClusterRegistry {

    enum ClusterType {
        User,
        System
    }

    @Nullable
    ClusterDescription findCluster(String poolLabel, String zone, ClusterType type);

    ClusterDescription getCluster(String clusterId);

    String getClusterPodsCidr(String clusterId);

    record ClusterDescription(
        String clusterId,
        HostAndPort masterAddress,
        String masterCert,
        ClusterType type
    ) {}
}
