package ai.lzy.allocator.alloc.impl.kuber;

import ai.lzy.allocator.model.Vm;
import ai.lzy.allocator.model.Workload;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;

@Singleton
@Requires(property = "allocator.kuber-tunnel-allocator.enabled", value = "false", defaultValue = "false")
public class DummyTunnelAllocator implements TunnelAllocator {
    @Override
    public String allocateTunnel(Vm.Spec vmSpec) {
        throw new UnsupportedOperationException(
            "ks8 tunnel allocator needs property allocator.kuber-tunnel-allocator.enabled=true"
        );
    }

    @Override
    public void deallocateTunnel(String podName) {
        throw new UnsupportedOperationException(
            "ks8 tunnel allocator needs property allocator.kuber-tunnel-allocator.enabled=true"
        );
    }

    @Override
    public Workload createRequestTunnelWorkload(String remoteV6, String poolLabel, String zone, int tunnelIndex) {
        throw new UnsupportedOperationException(
            "ks8 tunnel allocator needs property allocator.kuber-tunnel-allocator.enabled=true"
        );
    }
}
