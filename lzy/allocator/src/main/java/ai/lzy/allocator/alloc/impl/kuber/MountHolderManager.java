package ai.lzy.allocator.alloc.impl.kuber;

import ai.lzy.allocator.model.ClusterPod;
import ai.lzy.allocator.model.DynamicMount;
import ai.lzy.allocator.model.PodPhase;
import ai.lzy.allocator.model.Vm;
import ai.lzy.allocator.model.VolumeClaim;

public interface MountHolderManager {

    ClusterPod allocateMountHolder(Vm.Spec mountToVm);

    void attachVolume(ClusterPod clusterPod, DynamicMount mount, VolumeClaim claim);

    void detachVolume(ClusterPod clusterPod, String mountName);

    void deallocateMountHolder(ClusterPod clusterPod);

    PodPhase checkPodPhase(ClusterPod clusterPod);
}
