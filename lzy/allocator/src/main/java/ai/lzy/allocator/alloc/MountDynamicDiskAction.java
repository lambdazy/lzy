package ai.lzy.allocator.alloc;

import ai.lzy.allocator.alloc.impl.kuber.MountHolderManager;
import ai.lzy.allocator.model.ClusterPod;
import ai.lzy.allocator.model.DynamicMount;
import ai.lzy.allocator.model.PodPhase;
import ai.lzy.allocator.model.Vm;
import ai.lzy.allocator.model.Volume;
import ai.lzy.allocator.model.VolumeClaim;
import ai.lzy.allocator.model.debug.InjectedFailures;
import ai.lzy.allocator.volume.VolumeManager;
import ai.lzy.longrunning.Operation;
import ai.lzy.longrunning.OperationRunnerBase;
import ai.lzy.model.db.TransactionHandle;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.grpc.Status;
import org.jetbrains.annotations.NotNull;

import java.net.HttpURLConnection;
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

    public MountDynamicDiskAction(String opId, Vm vm, DynamicMount dynamicMount, AllocationContext allocationContext) {
        super(opId, String.format("mount disk %s to VM %s", dynamicMount.volumeDescription().diskId(), vm.vmId()),
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
        return List.of(this::createVolume, this::createVolumeClaim, this::setVolumeClaim, this::attachVolumeToPod,
            this::setMountName, this::waitForPod, this::checkIfVmStillExists);
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
        return allocationContext.createUnmountAction(vm, dynamicMount, tx);
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
        if (this.volume != null) {
            return StepResult.ALREADY_DONE;
        }

        try {
            this.volume = volumeManager.createOrGet(dynamicMount.clusterId(), dynamicMount.volumeDescription());
        } catch (Exception e) {
            log().error("{} Couldn't create volume {}", dynamicMount.volumeDescription(), e);
            return StepResult.RESTART;
        }

        return StepResult.CONTINUE;
    }

    private StepResult createVolumeClaim() {
        if (volumeClaim != null) {
            return StepResult.ALREADY_DONE;
        }

        try {
            this.volumeClaim = volumeManager.createClaim(volume);
        } catch (Exception e) {
            log().error("{} Couldn't create volume claim {}", logPrefix(), dynamicMount.volumeDescription(), e);
            return StepResult.RESTART;
        }

        return StepResult.CONTINUE;
    }

    private StepResult setVolumeClaim() {
        if (dynamicMount.volumeClaimId() != null) {
            return StepResult.ALREADY_DONE;
        }

        try {
            var update = DynamicMount.Update.builder()
                .volumeClaimId(volumeClaim.id())
                .build();
            withRetries(log(), () -> allocationContext.dynamicMountDao().update(dynamicMount.id(), update, null));
            this.dynamicMount = dynamicMount.apply(update);
        } catch (Exception e) {
            log().error("{} Couldn't set volume claim id {}", logPrefix(), dynamicMount.volumeDescription(), e);
            return StepResult.RESTART;
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
            if (e.getCode() == HttpURLConnection.HTTP_NOT_FOUND) {
                log().error("{} Mount pod {} is not found", logPrefix(), mountPodName);
                fail(Status.FAILED_PRECONDITION.withDescription("Mount pod " + mountPodName + " is not found"));
                return StepResult.FINISH;
            }
            log().error("{} Couldn't attach volume to pod {}", logPrefix(), dynamicMount.volumeDescription(), e);
            return StepResult.RESTART;
        } catch (Exception e) {
            log().error("{} Couldn't attach volume to pod {}", logPrefix(), dynamicMount.volumeDescription(), e);
            return StepResult.RESTART;
        }

        return StepResult.CONTINUE;
    }

    private StepResult setMountName() {
        if (dynamicMount.mountName() != null) {
            return StepResult.ALREADY_DONE;
        }

        try {
            var update = DynamicMount.Update.builder()
                .state(DynamicMount.State.READY)
                .build();
            withRetries(log(), () -> allocationContext.dynamicMountDao().update(dynamicMount.id(), update, null));
            this.dynamicMount = dynamicMount.apply(update);
        } catch (Exception e) {
            log().error("{} Couldn't set mount name {}", logPrefix(), dynamicMount.volumeDescription(), e);
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
        } catch (Exception e) {
            log().error("{} Couldn't check pod phase {}", logPrefix(), dynamicMount.volumeDescription(), e);
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
                fail(Status.ABORTED.withDescription("Vm " + vm.vmId() + " is deleted"));
                return StepResult.FINISH;
            }
            switch (freshVm.status()) {
                case IDLE, ALLOCATING, RUNNING -> {
                    log().info("{} Vm {} is in status {}", logPrefix(), vm.vmId(), freshVm.status());
                    return StepResult.FINISH;
                }
                case DELETING -> {
                    log().error("{} Vm {} is deleting", logPrefix(), vm.vmId());
                    fail(Status.FAILED_PRECONDITION.withDescription("Vm " + vm.vmId() + " is deleting"));
                    return StepResult.FINISH;
                }
            }
        } catch (Exception e) {
            log().error("{} Couldn't get vm {}", logPrefix(), vm.vmId(), e);
            return StepResult.RESTART;
        }
        return StepResult.FINISH;
    }
}
