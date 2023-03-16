package ai.lzy.allocator.alloc.impl.kuber;

import ai.lzy.allocator.alloc.VmAllocator;
import ai.lzy.allocator.model.Vm;
import ai.lzy.allocator.model.Workload;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;

@Singleton
@Requires(property = "allocator.kuber-tunnel-allocator.enabled", value = "false", defaultValue = "false")
public class DummyTunnelAllocator implements TunnelAllocator {
    @Override
    public String allocateTunnelAgent(Vm.Spec vmSpec) {
        throw new UnsupportedOperationException(
            "ks8 tunnel allocator needs property allocator.kuber-tunnel-allocator.enabled=true"
        );
    }

    @Override
    public VmAllocator.Result deleteTunnel(String clusterId, String podName) {
        throw new UnsupportedOperationException(
            "ks8 tunnel allocator needs property allocator.kuber-tunnel-allocator.enabled=true"
        );
    }

    @Override
    public VmAllocator.Result deallocateTunnelAgent(String clusterId, String podName) {
        throw new UnsupportedOperationException(
            "ks8 tunnel allocator needs property allocator.kuber-tunnel-allocator.enabled=true"
        );
    }

    @Override
    public Workload createRequestTunnelWorkload(Vm.TunnelSettings tunnelSettings, String poolLabel, String zone) {
        throw new UnsupportedOperationException(
            "ks8 tunnel allocator needs property allocator.kuber-tunnel-allocator.enabled=true"
        );
    }
}
