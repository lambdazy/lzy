package ai.lzy.allocator.vmpool;

import com.google.common.net.HostAndPort;

import javax.annotation.Nullable;
import java.util.List;

public interface ClusterRegistry {

    enum ClusterType {
        User,
        System
    }

    @Nullable
    ClusterDescription findCluster(String poolLabel, String zone, ClusterType type);

    ClusterDescription getCluster(String clusterId);

    List<ClusterDescription> listClusters(ClusterType type);

    record ClusterDescription(
        String clusterId,
        HostAndPort masterAddress,
        String masterCert
    ) {}
}
