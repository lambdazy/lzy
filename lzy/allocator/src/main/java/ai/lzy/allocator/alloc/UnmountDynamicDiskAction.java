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
    @Nullable
    private Vm vm;
    private boolean mountPodStarted = false;
    private boolean volumeClaimDeletionRequested = false;
    private boolean volumeDeletionRequested = false;
    private boolean volumeUnmounted = false;
    private boolean skipVolumeDeletion = false;
    private boolean volumeDeleted = false;
    private boolean volumeClaimDeleted = false;
    private ClusterPod updatedMountPod = null;
    private List<DynamicMount> activeMounts;
    @Nullable
    private Long nextId;
    private @Nullable String oldMountPod;

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
        return List.of(this::prepareActiveMounts, this::getNextMountPodId, this::createNewMountPod,
            this::waitForMountPod, this::updateVmMountPod, this::deleteOtherMountPods,
            this::unmountFromVM, this::countDynamicMounts, this::removeVolumeClaim, this::removeVolume,
            this::waitVolumeClaimDeletion, this::waitVolumeDeletion, this::deleteMount);
    }

    private StepResult prepareActiveMounts() {
        if (vmIsDeleting()) {
            return StepResult.ALREADY_DONE;
        }
        if (!dynamicMount.mounted()) {
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

    private StepResult getNextMountPodId() {
        if (vmIsDeleting()) {
            return StepResult.ALREADY_DONE;
        }
        if (nextId != null) {
            return StepResult.ALREADY_DONE;
        }
        if (activeMounts.isEmpty()) {
            return StepResult.ALREADY_DONE;
        }

        try {
            nextId = withRetries(log(), () -> allocationContext.vmDao().getNextMountPodId(vm.vmId(), null));
        } catch (Exception e) {
            log().error("{} Couldn't get next mount id for vm", logPrefix(), e);
            return StepResult.CONTINUE;
        }
        return StepResult.CONTINUE;
    }

    private StepResult createNewMountPod() {
        if (vmIsDeleting()) {
            return StepResult.ALREADY_DONE;
        }
        if (!dynamicMount.mounted()) {
            return StepResult.ALREADY_DONE;
        }
        if (nextId == null) {
            return StepResult.ALREADY_DONE;
        }
        if (activeMounts.isEmpty()) {
            return StepResult.ALREADY_DONE;
        }

        var dynamicMounts = activeMounts.stream()
            .filter(x -> !x.id().equals(dynamicMount.id()))
            .toList();
        log().info("{} Creating mount pod with {} mounts", logPrefix(), dynamicMounts.size());
        try {
            updatedMountPod = allocationContext.mountHolderManager().allocateMountHolder(vm.spec(), dynamicMounts,
                nextId.toString());
        } catch (KubernetesClientException e) {
            log().error("{} Failed to create pod", logPrefix(), e);
            if (KuberUtils.isNotRetryable(e)) {
                return StepResult.CONTINUE;
            }
            return StepResult.RESTART;
        }

        return StepResult.CONTINUE;
    }

    private StepResult waitForMountPod() {
        if (vmIsDeleting()) {
            return StepResult.ALREADY_DONE;
        }
        if (!dynamicMount.mounted()) {
            return StepResult.ALREADY_DONE;
        }
        if (updatedMountPod == null) {
            return StepResult.ALREADY_DONE;
        }
        if (mountPodStarted) {
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
                    log().error("{} Mount pod {} is in unexpected state {}", logPrefix(), updatedMountPod.podName(),
                        podPhase);
                    return StepResult.CONTINUE;
            }
        } catch (KubernetesClientException e) {
            log().error("{} Failed to check mount pod {} state", logPrefix(), updatedMountPod.podName(), e);
            if (KuberUtils.isNotRetryable(e)) {
                return StepResult.CONTINUE;
            }
            return StepResult.RESTART;
        }
        return StepResult.CONTINUE;
    }

    private StepResult updateVmMountPod() {
        if (vmIsDeleting()) {
            return StepResult.ALREADY_DONE;
        }
        if (!dynamicMount.mounted()) {
            return StepResult.ALREADY_DONE;
        }
        if (updatedMountPod == null) {
            return StepResult.ALREADY_DONE;
        }
        if (Objects.equals(vm.instanceProperties().mountPodName(), updatedMountPod.podName())) {
            return StepResult.ALREADY_DONE;
        }

        log().info("{} Updating vm with new mount pod {}", logPrefix(), updatedMountPod.podName());
        try {
            withRetries(log(), () -> {
                try (var tx = TransactionHandle.create(allocationContext.storage())) {
                    allocationContext.vmDao().setMountPodAndIncrementNextId(vm.vmId(), updatedMountPod.podName(), tx);
                    var mountedUpdate = DynamicMount.Update.builder()
                        .mounted(false)
                        .build();
                    allocationContext.dynamicMountDao().update(dynamicMount.id(), mountedUpdate, tx);
                    tx.commit();
                }
            });
            oldMountPod = vm.instanceProperties().mountPodName();
            vm = vm.withMountPod(updatedMountPod.podName());
        } catch (Exception e) {
            log().error("{} Failed to update vm with new mount pod {}", logPrefix(), updatedMountPod.podName(), e);
        }
        return StepResult.CONTINUE;
    }

    private StepResult deleteOtherMountPods() {
        if (vmIsDeleting()) {
            return StepResult.ALREADY_DONE;
        }
        if (!dynamicMount.mounted()) {
            return StepResult.ALREADY_DONE;
        }
        if (oldMountPod == null) {
            return StepResult.ALREADY_DONE;
        }

        log().info("{} Deleting old mount pod {}", logPrefix(), oldMountPod);
        try {
            allocationContext.mountHolderManager().deallocateOtherMountPods(vm.vmId(), updatedMountPod);
        } catch (KubernetesClientException e) {
            log().error("{} Couldn't delete mount pod {} for {}", logPrefix(), oldMountPod, dynamicMount.id(), e);
            if (KuberUtils.isNotRetryable(e)) {
                return StepResult.CONTINUE;
            }
            return StepResult.RESTART;
        }

        return StepResult.CONTINUE;
    }

    private StepResult unmountFromVM() {
        if (vmIsDeleting()) {
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
            log().error("{} Failed to unmount volume. Status: {}", logPrefix(), e.getCode(), e);
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
                skipVolumeDeletion = true;
            }
        } catch (Exception e) {
            log().error("{} Failed to count mounts by volume claim", logPrefix(), e);
        }
        return StepResult.CONTINUE;
    }


    private StepResult removeVolumeClaim() {
        if (volumeClaimDeletionRequested || skipVolumeDeletion) {
            return StepResult.ALREADY_DONE;
        }
        log().info("{} Deleting volume claim {}", logPrefix(), dynamicMount.volumeClaimName());
        try {
            allocationContext.volumeManager().deleteClaim(dynamicMount.clusterId(), dynamicMount.volumeClaimName());
            volumeClaimDeletionRequested = true;
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
        if (volumeDeletionRequested) {
            return StepResult.ALREADY_DONE;
        }

        if (skipVolumeDeletion) {
            return StepResult.ALREADY_DONE;
        }

        log().info("{} Deleting volume {}", logPrefix(), dynamicMount.volumeName());
        try {
            allocationContext.volumeManager().delete(dynamicMount.clusterId(), dynamicMount.volumeName());
            volumeDeletionRequested = true;
        } catch (KubernetesClientException e) {
            log().error("{} Failed to delete volume {}", logPrefix(), dynamicMount.volumeName(), e);
            if (KuberUtils.isNotRetryable(e)) {
                return StepResult.CONTINUE;
            }
            return StepResult.RESTART;
        }
        return StepResult.CONTINUE;
    }

    private StepResult waitVolumeClaimDeletion() {
        if (skipVolumeDeletion) {
            return StepResult.ALREADY_DONE;
        }
        if (volumeClaimDeleted) {
            return StepResult.ALREADY_DONE;
        }

        log().info("{} Waiting for volume claim {} to be deleted", logPrefix(), dynamicMount.volumeClaimName());
        try {
            var claim = allocationContext.volumeManager().getClaim(dynamicMount.clusterId(),
                dynamicMount.volumeClaimName());
            if (claim != null) {
                return StepResult.RESTART;
            }
            volumeClaimDeleted = true;
        } catch (KubernetesClientException e) {
            log().error("{} Failed to wait for volume claim {} to be deleted", logPrefix(),
                dynamicMount.volumeClaimName(), e);
            if (KuberUtils.isNotRetryable(e)) {
                return StepResult.CONTINUE;
            }
            return StepResult.RESTART;
        }
        return StepResult.CONTINUE;
    }

    private StepResult waitVolumeDeletion() {
        if (skipVolumeDeletion) {
            return StepResult.ALREADY_DONE;
        }
        if (volumeDeleted) {
            return StepResult.ALREADY_DONE;
        }

        log().info("{} Waiting for volume {} to be deleted", logPrefix(), dynamicMount.volumeName());
        try {
            var volume = allocationContext.volumeManager().get(dynamicMount.clusterId(), dynamicMount.volumeName());
            if (volume != null) {
                return StepResult.RESTART;
            }
            volumeDeleted = true;
        } catch (KubernetesClientException e) {
            log().error("{} Failed to wait for volume {} to be deleted", logPrefix(), dynamicMount.volumeName(), e);
            if (KuberUtils.isNotRetryable(e)) {
                return StepResult.CONTINUE;
            }
            return StepResult.RESTART;
        }
        return StepResult.CONTINUE;
    }

    private StepResult deleteMount() {
        try {
            withRetries(log(), () -> {
                try (var tx = TransactionHandle.create(allocationContext.storage())) {
                    allocationContext.dynamicMountDao().delete(dynamicMount.id(), tx);
                    completeOperation(null, Any.pack(VmAllocatorApi.UnmountResponse.getDefaultInstance()), tx);
                    tx.commit();
                }
            });
        } catch (Exception e) {
            log().error("{} Failed to delete mount {}", logPrefix(), dynamicMount.id(), e);
        }
        return StepResult.FINISH;
    }

    private boolean vmIsDeleting() {
        return vm == null || vm.status() == Vm.Status.DELETING;
    }
}
