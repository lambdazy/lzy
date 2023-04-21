package ai.lzy.allocator.alloc;

import ai.lzy.allocator.exceptions.InvalidConfigurationException;
import ai.lzy.allocator.model.ClusterPod;
import ai.lzy.allocator.model.DynamicMount;
import ai.lzy.allocator.model.Vm;
import ai.lzy.allocator.util.KuberUtils;
import ai.lzy.logs.LogContextKey;
import ai.lzy.longrunning.OperationRunnerBase;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.v1.VmAllocatorApi;
import com.google.protobuf.Any;
import io.fabric8.kubernetes.client.KubernetesClientException;
import jakarta.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import static ai.lzy.model.db.DbHelper.withRetries;

public final class UnmountDynamicDiskAction extends OperationRunnerBase {

    private final AllocationContext allocationContext;
    private final DynamicMount dynamicMount;
    private Vm vm;
    private boolean mountPodStarted = false;
    private boolean volumeClaimDeleted = false;
    private boolean volumeDeleted = false;
    private boolean volumeUnmounted = false;
    private boolean mountDeleted = false;
    private boolean skipClaimDeletion = false;
    private ClusterPod updatedMountPod = null;
    private List<DynamicMount> activeMounts;

    public UnmountDynamicDiskAction(@Nullable Vm vm, DynamicMount dynamicMount,
                                    AllocationContext allocationContext)
    {
        super(dynamicMount.unmountOperationId(), description(vm, dynamicMount), allocationContext.storage(),
            allocationContext.operationsDao(), allocationContext.executor());
        this.vm = vm;
        this.dynamicMount = dynamicMount;
        this.allocationContext = allocationContext;
    }

    public static String description(@Nullable Vm vm, DynamicMount mount) {
        if (vm == null) {
            return "Remove mount %s".formatted(mount.id());
        }
        return "Unmount volume %s from vm %s".formatted(mount.id(), vm.vmId());
    }

    @Override
    protected void notifyFinished() {
        log().info("{} Finished unmounting volume {}", logPrefix(), dynamicMount.id());
    }

    @Override
    protected Map<String, String> prepareLogContext() {
        var ctx =  super.prepareLogContext();
        if (vm != null) {
            ctx.put(LogContextKey.VM_ID, vm.vmId());
        }
        ctx.put(LogContextKey.DYNAMIC_MOUNT_ID, dynamicMount.id());
        return ctx;
    }

    @Override
    protected List<Supplier<StepResult>> steps() {
        return List.of(this::prepareActiveMounts, this::recreateMountPod, this::updateVmMountPod, this::waitForMountPod,
            this::unmountFromVM, this::countDynamicMounts, this::removeVolumeClaim, this::removeVolume,
            this::deleteMount);
    }

    private StepResult prepareActiveMounts() {
        if (vm == null) {
            return StepResult.ALREADY_DONE;
        }

        if (activeMounts != null) {
            return StepResult.ALREADY_DONE;
        }

        try {
            activeMounts = withRetries(log(), () -> allocationContext.dynamicMountDao()
                .getByVmAndStates(vm.vmId(), List.of(DynamicMount.State.READY), null));
            var mountIds = activeMounts.stream().map(DynamicMount::id).toList();
            log().info("{} Found {} active mounts for vm: {}", logPrefix(), activeMounts.size(),
                mountIds);
        } catch (Exception e) {
            log().error("{} Failed to update vm with active mount {}", logPrefix(), dynamicMount.id(), e);
        }
        return StepResult.CONTINUE;
    }

    private StepResult recreateMountPod() {
        if (vm == null) {
            return StepResult.ALREADY_DONE;
        }

        if (updatedMountPod != null) {
            return StepResult.ALREADY_DONE;
        }

        var mountPodName = vm.instanceProperties().mountPodName();
        if (mountPodName == null) {
            return StepResult.ALREADY_DONE;
        }

        var dynamicMounts = activeMounts.stream()
            .filter(x -> !x.id().equals(dynamicMount.id()))
            .toList();
        log().info("{} Recreating mount pod {} with {} mounts", logPrefix(), mountPodName, dynamicMounts.size());
        try {
            updatedMountPod = allocationContext.mountHolderManager().recreateWith(vm.spec(),
                ClusterPod.of(dynamicMount.clusterId(), mountPodName), dynamicMounts);
        } catch (KubernetesClientException e) {
            log().error("{} Failed to detach volume {} from pod {}", logPrefix(), dynamicMount.mountName(),
                mountPodName, e);
            if (KuberUtils.isNotRetryable(e)) {
                return StepResult.CONTINUE;
            }
            return StepResult.RESTART;
        }
        return StepResult.CONTINUE;
    }

    private StepResult updateVmMountPod() {
        if (vm == null) {
            return StepResult.ALREADY_DONE;
        }

        if (Objects.equals(vm.instanceProperties().mountPodName(), updatedMountPod.podName())) {
            return StepResult.ALREADY_DONE;
        }

        log().info("{} Updating vm with new mount pod {}", logPrefix(), updatedMountPod.podName());
        try {
            withRetries(log(), () ->
                allocationContext.vmDao().setMountPod(vm.vmId(), updatedMountPod.podName(), null));
            vm = vm.withMountPod(updatedMountPod.podName());
        } catch (Exception e) {
            log().error("{} Failed to update vm with new mount pod {}", logPrefix(), updatedMountPod.podName(), e);
        }
        return StepResult.CONTINUE;
    }

    private StepResult waitForMountPod() {
        if (vm == null) {
            return StepResult.ALREADY_DONE;
        }

        if (updatedMountPod == null) {
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
            var podPhase = allocationContext.mountHolderManager().checkPodPhase(updatedMountPod);
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
        } catch (KubernetesClientException e) {
            log().error("{} Failed to check mount pod {} state", logPrefix(), mountPodName, e);
            if (KuberUtils.isNotRetryable(e)) {
                return StepResult.CONTINUE;
            }
            return StepResult.RESTART;
        }
        return StepResult.CONTINUE;
    }

    private StepResult unmountFromVM() {
        if (vm == null || vm.status() == Vm.Status.DELETING) {
            return StepResult.ALREADY_DONE;
        }

        if (volumeUnmounted) {
            return StepResult.ALREADY_DONE;
        }

        log().info("{} Unmounting mount point {}", logPrefix(), dynamicMount.mountPath());
        try {
            allocationContext.allocator().unmountFromVm(vm, dynamicMount.mountPath());
            volumeUnmounted = true;
        } catch (InvalidConfigurationException e) {
            log().error("{} Failed to unmount volume", logPrefix(), e);
        } catch (KubernetesClientException e) {
            log().error("{} Failed to unmount volume", logPrefix(), e);
            if (KuberUtils.isNotRetryable(e)) {
                return StepResult.CONTINUE;
            }
            return StepResult.RESTART;
        }
        return StepResult.CONTINUE;
    }

    private StepResult countDynamicMounts() {
        log().info("{} Counting dynamic mounts for volume claim {}", logPrefix(), dynamicMount.volumeClaimName());
        try {
            var count = withRetries(log(), () -> allocationContext.dynamicMountDao()
                .countVolumeClaimUsages(dynamicMount.clusterId(), dynamicMount.volumeClaimName(), null));

            if (count > 1) {
                skipClaimDeletion = true;
            }
        } catch (Exception e) {
            log().error("{} Failed to count mounts by volume claim", logPrefix(), e);
        }
        return StepResult.CONTINUE;
    }


    private StepResult removeVolumeClaim() {
        if (volumeClaimDeleted || skipClaimDeletion) {
            return StepResult.ALREADY_DONE;
        }
        log().info("{} Deleting volume claim {}", logPrefix(), dynamicMount.volumeClaimName());
        try {
            allocationContext.volumeManager().deleteClaim(dynamicMount.clusterId(), dynamicMount.volumeClaimName());
            volumeClaimDeleted = true;
        } catch (KubernetesClientException e) {
            log().error("{} Failed to delete volume claim {}", logPrefix(), dynamicMount.volumeClaimName(), e);
            if (KuberUtils.isNotRetryable(e)) {
                return StepResult.CONTINUE;
            }
            return StepResult.RESTART;
        }

        return StepResult.CONTINUE;
    }

    private StepResult removeVolume() {
        if (volumeDeleted) {
            return StepResult.ALREADY_DONE;
        }

        if (skipClaimDeletion) {
            return StepResult.ALREADY_DONE;
        }

        log().info("{} Deleting volume {}", logPrefix(), dynamicMount.volumeName());
        try {
            allocationContext.volumeManager().delete(dynamicMount.clusterId(), dynamicMount.volumeName());
            volumeDeleted = true;
        } catch (KubernetesClientException e) {
            log().error("{} Failed to delete volume {}", logPrefix(), dynamicMount.volumeName(), e);
            if (KuberUtils.isNotRetryable(e)) {
                return StepResult.CONTINUE;
            }
            return StepResult.RESTART;
        }
        return StepResult.CONTINUE;
    }

    private StepResult deleteMount() {
        if (mountDeleted) {
            return StepResult.ALREADY_DONE;
        }

        try {
            withRetries(log(), () -> {
                try (var tx = TransactionHandle.create(allocationContext.storage())) {
                    allocationContext.dynamicMountDao().delete(dynamicMount.id(), tx);
                    completeOperation(null, Any.pack(VmAllocatorApi.UnmountResponse.getDefaultInstance()), tx);
                    tx.commit();
                }
            });
            mountDeleted = true;
        } catch (Exception e) {
            log().error("{} Failed to delete mount {}", logPrefix(), dynamicMount.id(), e);
        }
        return StepResult.FINISH;
    }
}
