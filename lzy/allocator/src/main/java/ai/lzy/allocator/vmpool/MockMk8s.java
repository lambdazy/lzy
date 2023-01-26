package ai.lzy.allocator.vmpool;

import com.google.common.net.HostAndPort;
import io.micronaut.context.annotation.Primary;
import io.micronaut.context.annotation.Requires;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import javax.inject.Singleton;

@Singleton
@Primary
@Requires(property = "allocator.mock-mk8s.enabled", value = "true")
public class MockMk8s implements VmPoolRegistry, ClusterRegistry {

    private final Map<String, ClusterDescription> labelsToClusters = new ConcurrentHashMap<>(Map.of(
        "S", new ClusterDescription(
            "S-" + UUID.randomUUID(),
            HostAndPort.fromString("localhost:1256"),
            "", ClusterType.User),
        "M", new ClusterDescription(
            "M-" + UUID.randomUUID(),
            HostAndPort.fromString("localhost:1256"),
            "", ClusterType.User)
    ));
    private final Map<String, ClusterDescription> idsToClusters;

    public MockMk8s() {
        this.idsToClusters = new ConcurrentHashMap<>();
        labelsToClusters.forEach(
            (s, clusterDescription) -> idsToClusters.put(clusterDescription.clusterId(), clusterDescription));
    }

    @Nullable
    @Override
    public ClusterDescription findCluster(String poolLabel, String zone, ClusterType type) {
        return labelsToClusters.get(poolLabel);
    }

    @Override
    public ClusterDescription getCluster(String clusterId) {
        return idsToClusters.get(clusterId);
    }

    @Override
    public String getClusterPodsCidr(String clusterId) {
        return "10.20.0.0/16";
    }

    @Override
    public Map<String, VmPoolSpec> getSystemVmPools() {
        return null;
    }

    @Override
    public Map<String, VmPoolSpec> getUserVmPools() {
        return Map.of("s", new VmPoolSpec("s", CpuTypes.CASCADE_LAKE.value(),
            2, GpuTypes.NO_GPU.value(), 0, 4, Set.of("ru-central1-a")));
    }
}
