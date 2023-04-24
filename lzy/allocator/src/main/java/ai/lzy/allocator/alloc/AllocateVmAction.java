package ai.lzy.allocator.alloc;

import ai.lzy.allocator.exceptions.InvalidConfigurationException;
import ai.lzy.allocator.model.ClusterPod;
import ai.lzy.allocator.model.Vm;
import ai.lzy.allocator.model.debug.InjectedFailures;
import ai.lzy.allocator.util.KuberUtils;
import ai.lzy.longrunning.Operation;
import ai.lzy.longrunning.OperationRunnerBase;
import ai.lzy.longrunning.dao.OperationCompletedException;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.NotFoundException;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.grpc.Status;
import jakarta.annotation.Nullable;

import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.function.Supplier;

import static ai.lzy.model.db.DbHelper.withRetries;

public final class AllocateVmAction extends OperationRunnerBase {
    private Vm vm;
    private final AllocationContext allocationContext;
    private String tunnelPodName = null;
    private boolean kuberRequestDone = false;
    @Nullable
    private DeleteVmAction deleteVmAction = null;
    private boolean mountPodAllocated = false;
    private ClusterPod mountHolder;

    public AllocateVmAction(Vm vm, AllocationContext allocationContext, boolean restore) {
        super(vm.allocOpId(), "VM " + vm.vmId(), allocationContext.storage(), allocationContext.operationsDao(),
            allocationContext.executor());

        this.vm = vm;
        this.allocationContext = allocationContext;

        if (restore) {
            log().info("{} Restore allocation...", logPrefix());
        } else {
            log().info("{} Start allocation...", logPrefix());
        }

        allocationContext.metrics().runningAllocations.labels(vm.spec().poolLabel()).inc();
    }

    @Override
    protected List<Supplier<OperationRunnerBase.StepResult>> steps() {
        return List.of(this::allocateTunnel, this::allocateVm, this::allocateMountPod, this::setMountPod, this::waitVm);
    }

    @Override
    protected boolean isInjectedError(Error e) {
        return e instanceof InjectedFailures.TerminateException;
    }

    @Override
    protected void notifyExpired() {
        allocationContext.metrics().allocationError.inc();
        allocationContext.metrics().allocationTimeout.inc();
    }

    @Override
    protected void notifyFinished() {
        allocationContext.metrics().runningAllocations.labels(vm.poolLabel()).dec();

        if (deleteVmAction != null) {
            log().info("{} Submit DeleteVmAction operation {}", logPrefix(), deleteVmAction.id());
            allocationContext.startNew(deleteVmAction);
            deleteVmAction = null;
        }
    }

    @Override
    protected void onExpired(TransactionHandle tx) throws SQLException {
        prepareDeleteVmAction("Allocation op '%s' expired".formatted(vm.allocateState().operationId()), tx);
    }

    @Override
    protected void onCompletedOutside(Operation op, TransactionHandle tx) throws SQLException {
        if (op.error() != null) {
            prepareDeleteVmAction("Operation failed: %s".formatted(op.error().getCode()), tx);
        } else {
            log().info("{} Allocation was successfully completed", logPrefix());
        }
    }

    private StepResult allocateTunnel() {
        InjectedFailures.failAllocateVm3();

        if (vm.tunnelSettings() == null) {
            return StepResult.ALREADY_DONE;
        }

        if (vm.instanceProperties().tunnelPodName() != null) {
            return StepResult.ALREADY_DONE;
        }

        if (tunnelPodName == null) {
            try {
                tunnelPodName = allocationContext.tunnelAllocator().allocateTunnelAgent(vm.spec());
            } catch (Exception e) {
                allocationContext.metrics().allocationError.inc();
                log().error("{} Cannot allocate tunnel: {}", logPrefix(), e.getMessage());
                try {
                    fail(Status.INVALID_ARGUMENT.withDescription(e.getMessage()));
                    return StepResult.FINISH;
                } catch (OperationCompletedException ex) {
                    log().error("{} Cannot fail operation: already completed", logPrefix());
                    return StepResult.FINISH;
                } catch (NotFoundException ex) {
                    log().error("{} Cannot fail operation: not found", logPrefix());
                    return StepResult.FINISH;
                } catch (Exception ex) {
                    log().error("{} Cannot fail operation: {}. Retry later...", logPrefix(), ex.getMessage());
                    return StepResult.RESTART;
                }
            }
        }

        InjectedFailures.failAllocateVm4();

        try {
            withRetries(log(), () -> allocationContext.vmDao().setTunnelPod(vm.vmId(), tunnelPodName, null));
            vm = vm.withTunnelPod(tunnelPodName);
        } catch (Exception e) {
            log().error("{} Cannot save tunnel pod name {} for VM: {}. Retry later...",
                logPrefix(), tunnelPodName, e.getMessage());
            return StepResult.RESTART;
        }

        return StepResult.CONTINUE;
    }

    private StepResult allocateVm() {
        if (kuberRequestDone) {
            return StepResult.ALREADY_DONE;
        }

        InjectedFailures.failAllocateVm5();

        final var vmRef = new Vm.Ref(vm);
        try {
            var result = allocationContext.allocator().allocate(vmRef);
            vm = vmRef.vm();
            return switch (result.code()) {
                case SUCCESS -> {
                    kuberRequestDone = true;
                    yield StepResult.RESTART;
                }
                case RETRY_LATER -> StepResult.RESTART;
                case FAILED -> {
                    log().error("{} Fail allocation: {}", logPrefix(), result.message());
                    fail(Status.INTERNAL.withDescription(result.message()));
                    yield StepResult.FINISH;
                }
            };
        } catch (Exception e) {
            vm = vmRef.vm();
            log().error("{} Error during VM allocation: {}", logPrefix(), e.getMessage(), e);
            allocationContext.metrics().allocationError.inc();
            var status = e instanceof InvalidConfigurationException
                ? Status.INVALID_ARGUMENT.withDescription(e.getMessage())
                : Status.INTERNAL.withDescription(e.getMessage());
            try {
                fail(status);
                return StepResult.FINISH;
            } catch (OperationCompletedException ex) {
                log().error("{} Cannot fail operation: already completed", logPrefix());
                return StepResult.FINISH;
            } catch (NotFoundException ex) {
                log().error("{} Cannot fail operation: not found", logPrefix());
                return StepResult.FINISH;
            } catch (Exception ex) {
                log().error("{} Cannot fail operation: {}", logPrefix(), e.getMessage(), e);
                return StepResult.RESTART;
            }
        }
    }

    private StepResult allocateMountPod() {
        if (!allocationContext.mountConfig().isEnabled()) {
            return StepResult.ALREADY_DONE;
        }

        if (mountPodAllocated) {
            return StepResult.ALREADY_DONE;
        }

        log().info("{} Allocating mount pod for VM {}", logPrefix(), vm.vmId());
        try {
            mountHolder = allocationContext.mountHolderManager().allocateMountHolder(vm.spec());
            mountPodAllocated = true;
        } catch (KubernetesClientException e) {
            log().error("{} Cannot allocate mount holder for vm {}: {}", logPrefix(), vm.vmId(), e.getMessage());
            if (KuberUtils.isNotRetryable(e)) {
                try {
                    fail(Status.INTERNAL.withDescription(e.getMessage()));
                } catch (Exception ex) {
                    log().error("{} Cannot fail operation: {}", logPrefix(), ex.getMessage(), ex);
                }
                return StepResult.FINISH;
            }
            return StepResult.RESTART;
        }
        return StepResult.CONTINUE;
    }

    private StepResult setMountPod() {
        if (!allocationContext.mountConfig().isEnabled()) {
            return StepResult.ALREADY_DONE;
        }

        if (vm.instanceProperties().mountPodName() != null) {
            return StepResult.ALREADY_DONE;
        }

        var pod = mountHolder.podName();
        log().info("{} Setting mount pod name {} for VM", logPrefix(), pod);
        try {
            withRetries(log(), () -> allocationContext.vmDao().setMountPod(vm.vmId(), pod, null));
        } catch (Exception e) {
            log().error("{} Cannot save mount pod name {} for VM: {}. Retry later...",
                logPrefix(), pod, e.getMessage());
            return StepResult.RESTART;
        }
        vm = vm.withMountPod(pod);
        return StepResult.CONTINUE;
    }

    private StepResult waitVm() {
        log().info("{} ... waiting ...", logPrefix());
        return StepResult.RESTART.after(Duration.ofMillis(500));
    }

    private void prepareDeleteVmAction(String description, TransactionHandle tx) throws SQLException {
        if (deleteVmAction != null) {
            return;
        }

        deleteVmAction = allocationContext.createDeleteVmAction(vm, description, vm.allocateState().reqid(), tx);
    }

    private void fail(Status status) throws Exception {
        log().error("{} Fail VM allocation operation: {}", logPrefix(), status.getDescription());
        withRetries(log(), () -> {
            try (var tx = TransactionHandle.create(allocationContext.storage())) {
                failOperation(status, tx);
                prepareDeleteVmAction(status.getDescription(), tx);
                tx.commit();
            }
        });
    }
}
