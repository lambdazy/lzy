package ai.lzy.allocator.alloc.impl.kuber;

import ai.lzy.allocator.model.ClusterPod;
import ai.lzy.allocator.model.DynamicMount;
import ai.lzy.allocator.model.PodPhase;
import ai.lzy.allocator.model.Vm;

import java.util.List;

public interface MountHolderManager {

    ClusterPod allocateMountHolder(Vm.Spec mountToVm, List<DynamicMount> mounts, String suffix);

    void deallocateMountHolder(ClusterPod clusterPod);

    void deallocateOtherMountPods(String vmId, ClusterPod podToKeep);

    void deallocateAllMountPods(Vm.Spec vmSpec);

    PodPhase checkPodPhase(ClusterPod clusterPod);
}
