package ai.lzy.allocator.test;

import ai.lzy.allocator.vmpool.ClusterRegistry;
import ai.lzy.allocator.vmpool.VmPoolRegistry;
import ai.lzy.allocator.vmpool.VmPoolSpec;
import io.micronaut.context.annotation.Primary;
import io.micronaut.context.annotation.Requires;
import java.util.Map;
import javax.inject.Singleton;
import org.jetbrains.annotations.Nullable;

@Singleton
@Primary
@Requires(property = "allocator.mock-mk8s.enabled", value = "true")
public class MockMk8s implements VmPoolRegistry, ClusterRegistry {

    @Nullable
    @Override
    public ClusterDescription findCluster(String poolLabel, String zone, ClusterType type) {
        return null;
    }

    @Override
    public ClusterDescription getCluster(String clusterId) {
        return null;
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
