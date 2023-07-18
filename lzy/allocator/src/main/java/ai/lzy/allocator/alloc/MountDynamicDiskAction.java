package ai.lzy.allocator.alloc;

import ai.lzy.allocator.alloc.impl.kuber.MountHolderManager;
import ai.lzy.allocator.model.*;
import ai.lzy.allocator.model.debug.InjectedFailures;
import ai.lzy.allocator.util.KuberUtils;
import ai.lzy.allocator.volume.VolumeManager;
import ai.lzy.logs.LogContextKey;
import ai.lzy.longrunning.Operation;
import ai.lzy.longrunning.OperationRunnerBase;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.v1.VmAllocatorApi;
import com.google.protobuf.Any;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.grpc.Status;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import static ai.lzy.model.db.DbHelper.withRetries;

public final class MountDynamicDiskAction extends OperationRunnerBase {
    private final AllocationContext allocationContext;
    private final VolumeManager volumeManager;
    private final MountHolderManager mountHolderManager;
    @Nonnull
    private Vm vm;
    private DynamicMount dynamicMount;
    private Volume volume;
    private VolumeClaim volumeClaim;
    private ClusterPod mountPod;
    private boolean podStarted;
    @Nullable
    private UnmountDynamicDiskAction unmountAction;
    private List<DynamicMount> activeMounts;
    private Long nextId;
    private boolean mountPodsDeleted;

    public MountDynamicDiskAction(Vm vm, DynamicMount dynamicMount, AllocationContext allocationContext) {
        super(dynamicMount.mountOperationId(), String.format("Mount %s to VM %s", dynamicMount.mountName(), vm.vmId()),
            allocationContext.storage(), allocationContext.operationsDao(), allocationContext.executor());
        this.dynamicMount = dynamicMount;
        this.vm = vm;
        this.allocationContext = allocationContext;
        this.volumeManager = allocationContext.volumeManager();
        this.mountHolderManager = allocationContext.mountHolderManager();

        log().info("{} Mount disk...", logPrefix());
    }

    @Override
    protected List<Supplier<StepResult>> steps() {
        return List.of(this::createVolumeIfNotExists, this::createVolumeClaimIfNotExists, this::setVolumeInfo,
            this::prepareActiveMounts, this::getNextMountPodId, this::createNewMountPod, this::waitForPod,
            this::updateVmMountPod, this::deleteOldMountPods, this::checkIfVmStillExists,
            this::setDynamicMountReady);
    }

    @Override
    protected boolean isInjectedError(Error e) {
        return e instanceof InjectedFailures.TerminateException;
    }

    @Override
    protected void notifyFinished() {
        if (unmountAction != null) {
            log().error("{} Failed to mount dynamic disk", logPrefix());
            try {
                allocationContext.startNew(unmountAction);
            } catch (Exception e) {
                log().error("{} Failed to start unmount dynamic disk action", logPrefix(), e);
            }
        } else {
            log().info("{} Mount disk finished", logPrefix());
        }
    }

    private void fail(Status status) {
        try {
            unmountAction = withRetries(log(), () -> {
                try (var tx = TransactionHandle.create(allocationContext.storage())) {
                    failOperation(status, tx);
                    var action = createUnmountAction(tx);
                    tx.commit();
                    return action;
                }
            });
        } catch (Exception e) {
            log().error("{} Failed to create unmount action", logPrefix(), e);
        }
    }

    @Override
    protected Map<String, String> prepareLogContext() {
        var ctx =  super.prepareLogContext();
        ctx.put(LogContextKey.VM_ID, vm.vmId());
        ctx.put(LogContextKey.DYNAMIC_MOUNT_ID, dynamicMount.id());
        return ctx;
    }

    private UnmountDynamicDiskAction createUnmountAction(TransactionHandle tx)
        throws SQLException
    {
        return allocationContext.createUnmountAction(vm, dynamicMount, tx).getLeft();
    }

    @Override
    protected void onExpired(TransactionHandle tx) throws SQLException {
        unmountAction = createUnmountAction(tx);
    }

    @Override
    protected void onCompletedOutside(Operation op, TransactionHandle tx) throws SQLException {
        if (op.error() != null) {
            unmountAction = createUnmountAction(tx);
        }
    }

    private StepResult createVolumeIfNotExists() {
        if (this.volume != null || dynamicMount.volumeName() != null) {
            return StepResult.ALREADY_DONE;
        }

        log().info("{} Creating volume for {}", logPrefix(), dynamicMount.volumeRequest());
        try {
            this.volume = volumeManager.create(dynamicMount.clusterId(), dynamicMount.volumeRequest());
        } catch (KubernetesClientException e) {
            log().error("{} Couldn't create volume for {}", logPrefix(), dynamicMount.volumeRequest(), e);
            if (KuberUtils.isNotRetryable(e)) {
                fail(Status.CANCELLED.withDescription("Couldn't create volume"));
                return StepResult.FINISH;
            }
            return StepResult.RESTART;
        }

        return StepResult.CONTINUE;
    }

    private StepResult createVolumeClaimIfNotExists() {
        if (volumeClaim != null || dynamicMount.volumeClaimName() != null) {
            return StepResult.ALREADY_DONE;
        }

        log().info("{} Creating volume claim for {}", logPrefix(), volume.name());
        try {
            this.volumeClaim = volumeManager.createClaim(dynamicMount.clusterId(), volume);
        } catch (KubernetesClientException e) {
            log().error("{} Couldn't create volume claim for {}", logPrefix(), volume.name(), e);
            if (KuberUtils.isNotRetryable(e)) {
                fail(Status.CANCELLED.withDescription("Couldn't create volume claim"));
                return StepResult.FINISH;
            }
            return StepResult.RESTART;
        }

        return StepResult.CONTINUE;
    }

    private StepResult setVolumeInfo() {
        if (dynamicMount.volumeName() != null && dynamicMount.volumeClaimName() != null) {
            return StepResult.ALREADY_DONE;
        }

        try {
            var update = DynamicMount.Update.builder()
                .volumeName(volume.name())
                .volumeClaimName(volumeClaim.name())
                .build();
            this.dynamicMount = withRetries(log(), () -> allocationContext.dynamicMountDao().update(dynamicMount.id(),
                update, null));
        } catch (Exception e) {
            log().error("{} Couldn't set volume info", logPrefix(), e);
            fail(Status.CANCELLED.withDescription("Couldn't set volume info"));
            return StepResult.FINISH;
        }
        return StepResult.CONTINUE;
    }

    private StepResult prepareActiveMounts() {
        if (activeMounts != null) {
            return StepResult.ALREADY_DONE;
        }

        try {
            activeMounts = withRetries(log(), () -> allocationContext.dynamicMountDao()
                .getByVmAndStates(vm.vmId(), List.of(DynamicMount.State.READY), null));
        } catch (Exception e) {
            log().error("{} Failed to read active mounts for vm", logPrefix(), e);
            fail(Status.CANCELLED.withDescription("Failed to read active mounts for vm"));
            return StepResult.FINISH;
        }
        return StepResult.CONTINUE;
    }

    private StepResult getNextMountPodId() {
        if (nextId != null) {
            return StepResult.ALREADY_DONE;
        }
        try {
            nextId = withRetries(log(), () -> allocationContext.vmDao().getNextMountPodId(vm.vmId(), null));
            Objects.requireNonNull(nextId, "nextId is null");
        } catch (Exception e) {
            log().error("{} Couldn't get next mount id for vm", logPrefix(), e);
            fail(Status.CANCELLED.withDescription("Couldn't get next mount id for vm"));
            return StepResult.FINISH;
        }
        return StepResult.CONTINUE;
    }

    private StepResult createNewMountPod() {
        if (mountPod != null) {
            return StepResult.ALREADY_DONE;
        }

        var dynamicMounts = new ArrayList<DynamicMount>(activeMounts.size() + 1);
        dynamicMounts.add(dynamicMount);
        dynamicMounts.addAll(activeMounts);

        log().info("{} Attaching mount {}", logPrefix(), dynamicMount.id());
        try {
            mountPod = allocationContext.mountHolderManager().allocateMountHolder(vm.spec(), dynamicMounts,
                nextId.toString());
        } catch (KubernetesClientException e) {
            log().error("{} Couldn't create mount pod for {}", logPrefix(), dynamicMount.id(), e);
            if (KuberUtils.isNotRetryable(e)) {
                fail(Status.CANCELLED.withDescription("Couldn't create mount pod for " + dynamicMount.id() + ": " +
                    e.getMessage()));
                return StepResult.FINISH;
            }
            return StepResult.RESTART;
        }

        return StepResult.CONTINUE;
    }

    private StepResult waitForPod() {
        if (podStarted) {
            return StepResult.ALREADY_DONE;
        }
        PodPhase podPhase;
        try {
            podPhase = mountHolderManager.checkPodPhase(mountPod);
        } catch (KubernetesClientException e) {
            log().error("{} Couldn't check pod {} phase", logPrefix(), mountPod, e);
            if (KuberUtils.isNotRetryable(e)) {
                fail(Status.CANCELLED.withDescription("Couldn't check pod " + mountPod.podName() + " phase: " +
                    e.getMessage()));
                return StepResult.FINISH;
            }
            return StepResult.RESTART;
        }
        log().debug("{} Pod {} is in phase {}", logPrefix(), mountPod.podName(), podPhase);
        return switch (podPhase) {
            case RUNNING -> {
                podStarted = true;
                yield StepResult.CONTINUE;
            }
            case PENDING -> StepResult.RESTART;
            case FAILED, SUCCEEDED, UNKNOWN -> {
                log().error("{} Pod {} is in phase {}", logPrefix(), mountPod.podName(), podPhase);
                fail(Status.FAILED_PRECONDITION
                    .withDescription("Pod " + mountPod.podName() + " is in phase " + podPhase));
                yield StepResult.FINISH;
            }
        };
    }

    private StepResult updateVmMountPod() {
        if (Objects.equals(vm.instanceProperties().mountPodName(), mountPod.podName())) {
            return StepResult.ALREADY_DONE;
        }

        log().info("{} Updating vm with new mount pod {}", logPrefix(), mountPod.podName());
        try {
            var mountedUpdate = DynamicMount.Update.builder()
                .mounted(true)
                .build();
            dynamicMount = withRetries(log(), () -> {
                try (var tx = TransactionHandle.create(allocationContext.storage())) {
                    allocationContext.vmDao().setMountPodAndIncrementNextId(vm.vmId(), mountPod.podName(), tx);
                    var updatedMount = allocationContext.dynamicMountDao().update(dynamicMount.id(),
                        mountedUpdate, tx);
                    tx.commit();
                    return updatedMount;
                }
            });
            vm = vm.withMountPod(mountPod.podName());
        } catch (Exception e) {
            log().error("{} Failed to update vm with new mount pod {}", logPrefix(), mountPod.podName(), e);
            fail(Status.CANCELLED.withDescription("Failed to update vm with new mount pod"));
        }
        return StepResult.CONTINUE;
    }

    private StepResult deleteOldMountPods() {
        if (mountPodsDeleted) {
            return StepResult.ALREADY_DONE;
        }

        log().info("{} Deleting other mount pods", logPrefix());
        try {
            allocationContext.mountHolderManager().deallocateOtherMountPods(vm.vmId(), mountPod);
            mountPodsDeleted = true;
        } catch (KubernetesClientException e) {
            log().error("{} Couldn't delete mount pods", logPrefix(), e);
            if (KuberUtils.isNotRetryable(e)) {
                fail(Status.CANCELLED.withDescription("Couldn't delete mount pods: " + e.getMessage()));
                return StepResult.FINISH;
            }
            return StepResult.RESTART;
        }

        return StepResult.CONTINUE;
    }

    private StepResult checkIfVmStillExists() {
        log().info("{} Checking if vm still exists", logPrefix());
        try {
            var freshVm = withRetries(log(), () -> allocationContext.vmDao().get(vm.vmId(), null));
            if (freshVm == null) {
                log().info("{} Vm is deleted", logPrefix());
                fail(Status.CANCELLED.withDescription("Vm " + vm.vmId() + " is deleted"));
                return StepResult.FINISH;
            }
            switch (freshVm.status()) {
                case IDLE, ALLOCATING, RUNNING -> {
                    log().info("{} Vm is in status {}", logPrefix(), freshVm.status());
                    return StepResult.CONTINUE;
                }
                case DELETING -> {
                    log().error("{} Vm is deleting", logPrefix());
                    fail(Status.FAILED_PRECONDITION.withDescription("Vm " + vm.vmId() + " is deleting"));
                    return StepResult.FINISH;
                }
            }
        } catch (Exception e) {
            log().error("{} Couldn't get vm", logPrefix(), e);
            fail(Status.CANCELLED.withDescription("Couldn't get vm " + vm.vmId() + ": " + e.getMessage()));
            return StepResult.FINISH;
        }
        return StepResult.CONTINUE;
    }

    private StepResult setDynamicMountReady() {
        if (dynamicMount.state() == DynamicMount.State.READY) {
            return StepResult.FINISH;
        }

        log().info("{} Setting mount {} state to READY", logPrefix(), dynamicMount.id());
        try {
            var update = DynamicMount.Update.builder()
                .state(DynamicMount.State.READY)
                .build();
            this.dynamicMount = withRetries(log(), () -> {
                try (var tx = TransactionHandle.create(allocationContext.storage())) {
                    var updatedMount = allocationContext.dynamicMountDao().update(dynamicMount.id(), update, tx);
                    completeOperation(null, Any.pack(VmAllocatorApi.MountResponse.newBuilder()
                        .setMount(dynamicMount.toProto())
                        .build()), tx);
                    tx.commit();
                    return updatedMount;
                }
            });
        } catch (Exception e) {
            log().error("{} Couldn't update mount {} state", logPrefix(), dynamicMount.id(), e);
            fail(Status.CANCELLED.withDescription("Couldn't update mount " + dynamicMount.id() + " state: " +
                e.getMessage()));
            return StepResult.FINISH;
        }
        return StepResult.FINISH;
    }
}
