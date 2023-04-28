package ai.lzy.allocator.alloc.impl.kuber;

import ai.lzy.allocator.model.ClusterPod;
import ai.lzy.allocator.model.DynamicMount;
import ai.lzy.allocator.model.PodPhase;
import ai.lzy.allocator.model.Vm;

import java.util.List;

public interface MountHolderManager {

    ClusterPod allocateMountHolder(Vm.Spec mountToVm, List<DynamicMount> mounts);

    ClusterPod recreateWith(Vm.Spec vm, ClusterPod currentPod, List<DynamicMount> mounts);

    void deallocateMountHolder(ClusterPod clusterPod);

    PodPhase checkPodPhase(ClusterPod clusterPod);
}
