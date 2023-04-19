package ai.lzy.allocator.vmpool;

import ai.lzy.common.IdGenerator;
import com.google.common.net.HostAndPort;
import io.micronaut.context.annotation.Primary;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Named;

import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.inject.Singleton;

import static ai.lzy.allocator.vmpool.CpuTypes.CASCADE_LAKE;
import static ai.lzy.allocator.vmpool.GpuTypes.NO_GPU;

@Singleton
@Primary
@Requires(property = "allocator.mock-mk8s.enabled", value = "true")
public class MockMk8s implements VmPoolRegistry, ClusterRegistry {

    private final Map<String, ClusterDescription> labelsToClusters;
    private final Map<String, ClusterDescription> idsToClusters;

    private final Map<String, VmPoolSpec> vmPools = Map.of(
        "s", new VmPoolSpec("s", CASCADE_LAKE.value(), 2, NO_GPU.value(), 0, 4, Set.of("ru-central1-a")),
        "m", new VmPoolSpec("m", CASCADE_LAKE.value(), 4, NO_GPU.value(), 0, 4, Set.of("ru-central1-a"))
    );

    public MockMk8s(@Named("AllocatorIdGenerator") IdGenerator idGenerator) {
        labelsToClusters = Map.of(
            "S", new ClusterDescription(
                idGenerator.generate("S-"),
                HostAndPort.fromString("localhost:1256"),
                "", ClusterType.User),
            "M", new ClusterDescription(
                idGenerator.generate("M-"),
                HostAndPort.fromString("localhost:1256"),
                "", ClusterType.User)
        );

        idsToClusters = labelsToClusters.entrySet().stream()
                .collect(Collectors.toMap(e -> e.getValue().clusterId(), Map.Entry::getValue));
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
        return vmPools;
    }

    @Nullable
    @Override
    public VmPoolSpec findPool(String poolLabel) {
        return vmPools.get(poolLabel.toLowerCase(Locale.ROOT));
    }
}
