package ai.lzy.allocator.vmpool;

import jakarta.annotation.Nullable;

import java.util.List;

public interface ClusterRegistry {

    enum ClusterType {
        User,
        System
    }

    @Nullable
    ClusterDescription findCluster(String poolLabel, String zone, ClusterType type);

    ClusterDescription getCluster(String clusterId);

    String getClusterPodsCidr(String clusterId);

    List<ClusterDescription> listClusters(ClusterType clusterType);

    record ClusterDescription(
        String clusterId,
        String masterAddress,
        String masterCert,
        ClusterType type
    ) {}
}
