package ai.lzy.allocator.alloc;

import ai.lzy.allocator.exceptions.InvalidConfigurationException;
import ai.lzy.allocator.model.ClusterPod;
import ai.lzy.allocator.model.DynamicMount;
import ai.lzy.allocator.model.Vm;
import ai.lzy.allocator.model.VolumeClaim;
import ai.lzy.longrunning.OperationRunnerBase;
import ai.lzy.model.db.TransactionHandle;
import io.fabric8.kubernetes.client.KubernetesClientException;

import java.util.List;
import java.util.function.Supplier;

import static ai.lzy.model.db.DbHelper.withRetries;

public final class UnmountDynamicDiskAction extends OperationRunnerBase {

    private final AllocationContext allocationContext;
    private final Vm vm;
    private final DynamicMount dynamicMount;
    private boolean mountPodRecreated;
    private VolumeClaim volumeClaim;
    private boolean mountPodStarted = false;
    private boolean volumeClaimDeleted = false;
    private boolean volumeDeleted = false;
    private boolean volumeUnmounted = false;

    public UnmountDynamicDiskAction(String opId, Vm vm, DynamicMount dynamicMount,
                                    AllocationContext allocationContext)
    {
        super(opId, "Unmount volume %s from vm %s".formatted(dynamicMount.volumeDescription().id(),
                vm.vmId()), allocationContext.storage(), allocationContext.operationsDao(),
            allocationContext.executor());
        this.vm = vm;
        this.dynamicMount = dynamicMount;
        this.allocationContext = allocationContext;
    }

    @Override
    protected List<Supplier<StepResult>> steps() {
        return List.of(this::recreateMountPod, this::waitForMountPod, this::unmountFromVM, this::removeVolumeClaim,
            this::removeVolume);
    }

    private StepResult recreateMountPod() {
        if (mountPodRecreated) {
            return StepResult.ALREADY_DONE;
        }

        var mountPodName = vm.instanceProperties().mountPodName();
        if (mountPodName == null) {
            return StepResult.ALREADY_DONE;
        }

        try {
            allocationContext.mountHolderManager().detachVolume(ClusterPod.of(dynamicMount.clusterId(), mountPodName),
                dynamicMount.mountName());
            withRetries(log(), () -> {
                try (var tx = TransactionHandle.create(allocationContext.storage())) {
                    allocationContext.dynamicMountDao().delete(dynamicMount.id(), tx);
                    tx.commit();
                }
            });
            mountPodRecreated = true;
        } catch (Exception e) {
            log().error("{} Failed to detach volume {} from pod {}", logPrefix(), dynamicMount.mountName(),
                mountPodName, e);
            return StepResult.RESTART;
        }
        return StepResult.CONTINUE;
    }

    private StepResult waitForMountPod() {
        if (!mountPodRecreated) {
            return StepResult.ALREADY_DONE;
        }

        if (mountPodStarted) {
            return StepResult.ALREADY_DONE;
        }

        var mountPodName = vm.instanceProperties().mountPodName();
        if (mountPodName == null) {
            log().warn("{} Mount pod name is null", logPrefix());
            return StepResult.ALREADY_DONE;
        }

        try {
            var podPhase = allocationContext.mountHolderManager()
                .checkPodPhase(ClusterPod.of(dynamicMount.clusterId(), mountPodName));
            switch (podPhase) {
                case RUNNING:
                    mountPodStarted = true;
                    return StepResult.CONTINUE;
                case PENDING:
                    return StepResult.RESTART;
                case SUCCEEDED:
                case FAILED:
                case UNKNOWN:
                    log().error("{} Mount pod {} is in unexpected state {}", logPrefix(), mountPodName, podPhase);
                    return StepResult.CONTINUE;
            }
        } catch (Exception e) {
            log().error("{} Failed to check mount pod {} state", logPrefix(), mountPodName, e);
        }
        return StepResult.CONTINUE;
    }

    private StepResult unmountFromVM() {
        if (vm.status() == Vm.Status.DELETING) {
            return StepResult.ALREADY_DONE;
        }

        if (volumeUnmounted) {
            return StepResult.ALREADY_DONE;
        }

        try {
            allocationContext.allocator().unmountFromVm(vm, dynamicMount.mountPath());
            volumeUnmounted = true;
        } catch (InvalidConfigurationException e) {
            log().error("{} Failed to unmount volume {} from vm {}", logPrefix(), dynamicMount.volumeDescription().id(),
                vm.vmId(), e);
        } catch (KubernetesClientException e) {
            log().error("{} Failed to unmount volume {} from vm {}", logPrefix(), dynamicMount.volumeDescription().id(),
                vm.vmId(), e);
            return StepResult.RESTART;
        }
        return StepResult.CONTINUE;
    }

    private StepResult removeVolumeClaim() {
        if (volumeClaimDeleted) {
            return StepResult.ALREADY_DONE;
        }
        try {
            volumeClaim = withRetries(log(), () -> {
                try (var tx = TransactionHandle.create(allocationContext.storage())) {
                    var mountPerVolumeClaim = allocationContext.dynamicMountDao()
                        .countForVolumeClaimId(dynamicMount.volumeClaimId(), tx);
                    if (mountPerVolumeClaim > 1) {
                        return null;
                    }
                    var claim = allocationContext.volumeClaimDao().get(dynamicMount.volumeClaimId(), tx);
                    tx.commit();
                    return claim;
                }
            });
            if (volumeClaim == null) {
                return StepResult.ALREADY_DONE;
            }

            allocationContext.volumeManager().deleteClaim(volumeClaim.clusterId(), volumeClaim.name());
            volumeClaimDeleted = true;
        } catch (Exception e) {
            log().error("{} Failed to delete volume claim {}", logPrefix(), dynamicMount.volumeDescription().id(), e);
            return StepResult.RESTART;
        }

        return StepResult.CONTINUE;
    }

    private StepResult removeVolume() {
        if (volumeDeleted) {
            return StepResult.ALREADY_DONE;
        }

        if (volumeClaim == null) {
            return StepResult.ALREADY_DONE;
        }

        try {
            allocationContext.volumeManager().delete(volumeClaim.clusterId(), volumeClaim.volumeName());
            volumeDeleted = true;
        } catch (Exception e) {
            log().error("{} Failed to delete volume {}", logPrefix(), dynamicMount.volumeDescription().id(), e);
            return StepResult.RESTART;
        }
        return StepResult.FINISH;
    }
}
