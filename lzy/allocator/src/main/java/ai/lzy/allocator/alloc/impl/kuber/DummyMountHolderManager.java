package ai.lzy.allocator.alloc.impl.kuber;

import ai.lzy.allocator.model.ClusterPod;
import ai.lzy.allocator.model.DynamicMount;
import ai.lzy.allocator.model.PodPhase;
import ai.lzy.allocator.model.Vm;
import ai.lzy.allocator.model.VolumeClaim;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;

@Singleton
@Requires(property = "allocator.mount.enabled", value = "false", defaultValue = "false")

public class DummyMountHolderManager implements MountHolderManager {
    @Override
    public ClusterPod allocateMountHolder(Vm.Spec mountToVm) {
        throw new UnsupportedOperationException("Mounts are not enabled");
    }

    @Override
    public void attachVolume(ClusterPod clusterPod, DynamicMount mount, VolumeClaim claim) {
        throw new UnsupportedOperationException("Mounts are not enabled");
    }

    @Override
    public void detachVolume(ClusterPod clusterPod, String mountName) {
        throw new UnsupportedOperationException("Mounts are not enabled");
    }

    @Override
    public void deallocateMountHolder(ClusterPod clusterPod) {
        throw new UnsupportedOperationException("Mounts are not enabled");
    }

    @Override
    public PodPhase checkPodPhase(ClusterPod clusterPod) {
        throw new UnsupportedOperationException("Mounts are not enabled");
    }
}
