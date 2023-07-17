package ai.lzy.allocator.vmpool;

import com.google.common.net.HostAndPort;
import jakarta.annotation.Nullable;

import java.util.Collection;
import java.util.Map;

public interface ClusterRegistry {

    enum ClusterType {
        User,
        System
    }

    enum PoolType {
        Cpu,
        Gpu
    }

    @Nullable
    ClusterDescription findCluster(String poolLabel, String zone, ClusterType type);

    ClusterDescription getCluster(String clusterId);

    Collection<ClusterDescription> getClusters();

    String getClusterPodsCidr(String clusterId);

    record ClusterDescription(
        String clusterId,
        HostAndPort masterAddress,
        String masterCert,
        ClusterType type,
        Map<String, PoolType> pools
    ) {}
}
