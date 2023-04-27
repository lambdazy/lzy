package ai.lzy.allocator.alloc;

import ai.lzy.allocator.alloc.impl.kuber.MountHolderManager;
import ai.lzy.allocator.model.*;
import ai.lzy.allocator.model.debug.InjectedFailures;
import ai.lzy.allocator.util.KuberUtils;
import ai.lzy.allocator.volume.VolumeManager;
import ai.lzy.longrunning.Operation;
import ai.lzy.longrunning.OperationRunnerBase;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.v1.VmAllocatorApi;
import com.google.protobuf.Any;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.grpc.Status;
import org.jetbrains.annotations.NotNull;

import java.sql.SQLException;
import java.util.List;
import java.util.function.Supplier;

import static ai.lzy.model.db.DbHelper.withRetries;

public final class MountDynamicDiskAction extends OperationRunnerBase {
    private final AllocationContext allocationContext;
    private final VolumeManager volumeManager;
    private final MountHolderManager mountHolderManager;
    private final Vm vm;
    private DynamicMount dynamicMount;
    private Volume volume;
    private VolumeClaim volumeClaim;
    private ClusterPod mountPod;
    private boolean podStarted;
    private UnmountDynamicDiskAction unmountAction;

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
        return List.of(this::createVolume, this::createVolumeClaim, this::setVolumeInfo, this::attachVolumeToPod,
            this::waitForPod, this::checkIfVmStillExists, this::setDynamicMountReady);
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
        }
    }

    private UnmountDynamicDiskAction createUnmountActionInTx(Status status) throws Exception {
        return withRetries(log(), () -> {
            try (var tx = TransactionHandle.create(allocationContext.storage())) {
                var action = createUnmountAction(status, tx);
                tx.commit();
                return action;
            }
        });
    }

    private void fail(Status status) {
        try {
            unmountAction = createUnmountActionInTx(status);
        } catch (Exception e) {
            log().error("{} Failed to create unmount action", logPrefix(), e);
        }
    }

    @NotNull
    private UnmountDynamicDiskAction createUnmountAction(Status status, TransactionHandle tx)
        throws SQLException
    {
        failOperation(status, tx);
        return allocationContext.createUnmountAction(vm, dynamicMount, tx).getLeft();
    }

    @Override
    protected void onExpired(TransactionHandle tx) throws SQLException {
        unmountAction = createUnmountAction(Status.DEADLINE_EXCEEDED, tx);
    }

    @Override
    protected void onCompletedOutside(Operation op, TransactionHandle tx) throws SQLException {
        if (op.error() != null) {
            unmountAction = createUnmountAction(op.error(), tx);
        }
    }

    private StepResult createVolume() {
        if (this.volume != null || dynamicMount.volumeName() != null) {
            return StepResult.ALREADY_DONE;
        }

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

    private StepResult createVolumeClaim() {
        if (volumeClaim != null || dynamicMount.volumeClaimName() != null) {
            return StepResult.ALREADY_DONE;
        }

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
            //todo what if dynamic mount is deleted?
            this.dynamicMount = withRetries(log(), () -> allocationContext.dynamicMountDao().update(dynamicMount.id(),
                update, null));
        } catch (Exception e) {
            log().error("{} Couldn't set volume info", logPrefix(), e);
            fail(Status.CANCELLED.withDescription("Couldn't set volume info"));
            return StepResult.FINISH;
        }
        return StepResult.CONTINUE;
    }

    private StepResult attachVolumeToPod() {
        if (mountPod != null) {
            return StepResult.ALREADY_DONE;
        }

        var mountPodName = vm.instanceProperties().mountPodName();
        if (mountPodName == null) {
            log().error("{} Mount pod name is not found for vm {}", logPrefix(), vm.vmId());
            fail(Status.FAILED_PRECONDITION.withDescription("Mount pod name is not found for vm " + vm.vmId()));
            return StepResult.FINISH;
        }

        var mountPod = ClusterPod.of(dynamicMount.clusterId(), mountPodName);

        try {
            mountHolderManager.attachVolume(mountPod, dynamicMount, volumeClaim);
            this.mountPod = mountPod;
        } catch (KubernetesClientException e) {
            log().error("{} Couldn't attach mount {} to pod {}", logPrefix(), dynamicMount.id(), mountPodName, e);
            if (KuberUtils.isNotRetryable(e)) {
                fail(Status.CANCELLED.withDescription("Couldn't attach mount " + dynamicMount.id() + " to pod " +
                    mountPodName + ": " + e.getMessage()));
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

    private StepResult checkIfVmStillExists() {
        try {
            var freshVm = withRetries(log(), () -> allocationContext.vmDao().get(vm.vmId(), null));
            if (freshVm == null) {
                log().info("{} Vm {} is deleted", logPrefix(), vm.vmId());
                fail(Status.CANCELLED.withDescription("Vm " + vm.vmId() + " is deleted"));
                return StepResult.FINISH;
            }
            switch (freshVm.status()) {
                case IDLE, ALLOCATING, RUNNING -> {
                    log().info("{} Vm {} is in status {}", logPrefix(), vm.vmId(), freshVm.status());
                    return StepResult.CONTINUE;
                }
                case DELETING -> {
                    log().error("{} Vm {} is deleting", logPrefix(), vm.vmId());
                    fail(Status.FAILED_PRECONDITION.withDescription("Vm " + vm.vmId() + " is deleting"));
                    return StepResult.FINISH;
                }
            }
        } catch (Exception e) {
            log().error("{} Couldn't get vm {}", logPrefix(), vm.vmId(), e);
            fail(Status.CANCELLED.withDescription("Couldn't get vm " + vm.vmId() + ": " + e.getMessage()));
            return StepResult.FINISH;
        }
        return StepResult.CONTINUE;
    }

    private StepResult setDynamicMountReady() {
        if (dynamicMount.state() == DynamicMount.State.READY) {
            return StepResult.FINISH;
        }

        try {
            var update = DynamicMount.Update.builder()
                .state(DynamicMount.State.READY)
                .build();
            withRetries(log(), () -> {
                try (var tx = TransactionHandle.create(allocationContext.storage())) {
                    //todo what if dynamic mount is null?
                    this.dynamicMount = allocationContext.dynamicMountDao().update(dynamicMount.id(), update, tx);
                    completeOperation(null, Any.pack(VmAllocatorApi.MountResponse.newBuilder()
                        .setMount(dynamicMount.toProto())
                        .build()), tx);
                    tx.commit();
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
