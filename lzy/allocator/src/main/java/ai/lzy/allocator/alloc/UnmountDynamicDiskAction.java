package ai.lzy.allocator.alloc;

import ai.lzy.allocator.exceptions.InvalidConfigurationException;
import ai.lzy.allocator.model.ClusterPod;
import ai.lzy.allocator.model.DynamicMount;
import ai.lzy.allocator.model.Vm;
import ai.lzy.allocator.util.KuberUtils;
import ai.lzy.longrunning.OperationRunnerBase;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.v1.VmAllocatorApi;
import com.google.protobuf.Any;
import io.fabric8.kubernetes.client.KubernetesClientException;
import jakarta.annotation.Nullable;

import java.util.List;
import java.util.function.Supplier;

import static ai.lzy.model.db.DbHelper.withRetries;

public final class UnmountDynamicDiskAction extends OperationRunnerBase {

    private final AllocationContext allocationContext;
    @Nullable
    private final Vm vm;
    private final DynamicMount dynamicMount;
    private boolean mountPodRecreated = false;
    private boolean mountPodStarted = false;
    private boolean volumeClaimDeleted = false;
    private boolean volumeDeleted = false;
    private boolean volumeUnmounted = false;
    private boolean mountDeleted = false;
    private boolean skipClaimDeletion = false;

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
    protected List<Supplier<StepResult>> steps() {
        return List.of(this::recreateMountPod, this::waitForMountPod, this::unmountFromVM, this::countDynamicMounts,
            this::removeVolumeClaim, this::removeVolume, this::deleteMount);
    }

    private StepResult recreateMountPod() {
        if (vm == null) {
            return StepResult.ALREADY_DONE;
        }

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
            mountPodRecreated = true;
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

    private StepResult waitForMountPod() {
        if (vm == null) {
            return StepResult.ALREADY_DONE;
        }

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
            var podPhase = allocationContext.mountHolderManager().checkPodPhase(ClusterPod.of(dynamicMount.clusterId(),
                mountPodName));
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

        try {
            allocationContext.allocator().unmountFromVm(vm, dynamicMount.mountPath());
            volumeUnmounted = true;
        } catch (InvalidConfigurationException e) {
            log().error("{} Failed to unmount volume {} from vm {}", logPrefix(), dynamicMount.id(),
                vm.vmId(), e);
        } catch (KubernetesClientException e) {
            log().error("{} Failed to unmount volume {} from vm {}", logPrefix(), dynamicMount.id(),
                vm.vmId(), e);
            if (KuberUtils.isNotRetryable(e)) {
                return StepResult.CONTINUE;
            }
            return StepResult.RESTART;
        }
        return StepResult.CONTINUE;
    }

    private StepResult countDynamicMounts() {
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
        try {
            allocationContext.volumeManager().deleteClaim(dynamicMount.clusterId(), dynamicMount.volumeClaimName());
            volumeClaimDeleted = true;
        } catch (KubernetesClientException e) {
            log().error("{} Failed to delete volume claim {}", logPrefix(), dynamicMount.volumeDescription().id(), e);
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

        try {
            allocationContext.volumeManager().delete(dynamicMount.clusterId(), dynamicMount.volumeName());
            volumeDeleted = true;
        } catch (KubernetesClientException e) {
            log().error("{} Failed to delete volume {}", logPrefix(), dynamicMount.volumeDescription().id(), e);
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
