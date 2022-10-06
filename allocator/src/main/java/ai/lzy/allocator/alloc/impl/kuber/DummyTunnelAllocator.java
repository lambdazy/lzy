package ai.lzy.allocator.alloc.impl.kuber;

import ai.lzy.allocator.model.Vm;
import ai.lzy.allocator.model.Workload;
import jakarta.inject.Singleton;

@Singleton
public class DummyTunnelAllocator implements TunnelAllocator {
    @Override
    public void allocateTunnel(Vm.Spec vmSpec) {
        throw new UnsupportedOperationException(
            "ks8 tunnel allocator needs property allocator.kuber-tunnel-allocator.enabled=true"
        );
    }

    @Override
    public Workload createRequestTunnelWorkload(String remoteV6, String poolLabel, String zone) {
        throw new UnsupportedOperationException(
            "ks8 tunnel allocator needs property allocator.kuber-tunnel-allocator.enabled=true"
        );
    }
}
