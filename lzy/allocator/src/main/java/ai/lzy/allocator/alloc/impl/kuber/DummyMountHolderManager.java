package ai.lzy.allocator.alloc.impl.kuber;

import ai.lzy.allocator.model.ClusterPod;
import ai.lzy.allocator.model.DynamicMount;
import ai.lzy.allocator.model.PodPhase;
import ai.lzy.allocator.model.Vm;
import io.micronaut.context.annotation.Requires;
import jakarta.annotation.Nonnull;
import jakarta.inject.Singleton;

import java.util.List;

@Singleton
@Requires(property = "allocator.mount.enabled", value = "false", defaultValue = "false")

public class DummyMountHolderManager implements MountHolderManager {
    @Override
    public ClusterPod allocateMountHolder(Vm.Spec mountToVm, List<DynamicMount> mounts, String suffix) {
        throw unsupported();
    }

    @Override
    public void deallocateMountHolder(ClusterPod clusterPod) {
        throw unsupported();
    }

    @Override
    public void deallocateOtherMountPods(String vmId, ClusterPod podToKeep) {
        throw unsupported();
    }

    @Override
    public void deallocateAllMountPods(Vm.Spec vmSpec) {
        throw unsupported();
    }

    @Override
    public PodPhase checkPodPhase(ClusterPod clusterPod) {
        throw unsupported();
    }

    @Nonnull
    private static UnsupportedOperationException unsupported() {
        return new UnsupportedOperationException("Mounts are not enabled");
    }
}
