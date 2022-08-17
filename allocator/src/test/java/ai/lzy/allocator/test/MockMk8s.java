package ai.lzy.allocator.test;

import ai.lzy.allocator.vmpool.ClusterRegistry;
import ai.lzy.allocator.vmpool.VmPoolRegistry;
import ai.lzy.allocator.vmpool.VmPoolSpec;
import com.google.common.net.HostAndPort;
import io.micronaut.context.annotation.Primary;
import io.micronaut.context.annotation.Requires;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import javax.inject.Singleton;
import org.jetbrains.annotations.Nullable;

@SuppressWarnings("UnstableApiUsage")
@Singleton
@Primary
@Requires(property = "allocator.mock-mk8s.enabled", value = "true")
public class MockMk8s implements VmPoolRegistry, ClusterRegistry {

    private final Map<String, ClusterDescription> labelsToClusters = new ConcurrentHashMap<>(Map.of(
        "S", new ClusterDescription(
            "S-" + UUID.randomUUID().toString(),
            HostAndPort.fromString("localhost:1256"),
            ""),
        "M", new ClusterDescription(
            "M-" + UUID.randomUUID().toString(),
            HostAndPort.fromString("localhost:1256"),
            "")
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
    public Map<String, VmPoolSpec> getSystemVmPools() {
        return null;
    }

    @Override
    public Map<String, VmPoolSpec> getUserVmPools() {
        return null;
    }
}
