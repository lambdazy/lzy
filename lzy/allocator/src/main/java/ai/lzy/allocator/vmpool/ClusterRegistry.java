package ai.lzy.allocator.vmpool;

import jakarta.annotation.Nullable;

import java.util.List;
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

    List<ClusterDescription> listClusters(@Nullable ClusterType clusterType);

    String getClusterPodsCidr(String clusterId);

    record ClusterDescription(
        String clusterId,
        String masterAddress,
        String masterCert,
        ClusterType type,
        Map<String, PoolType> pools
    ) {}
}
