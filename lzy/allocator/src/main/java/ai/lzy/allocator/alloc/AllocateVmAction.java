package ai.lzy.allocator.alloc;

import ai.lzy.allocator.exceptions.InvalidConfigurationException;
import ai.lzy.allocator.model.Vm;
import ai.lzy.allocator.model.debug.InjectedFailures;
import ai.lzy.longrunning.Operation;
import ai.lzy.longrunning.OperationRunnerBase;
import ai.lzy.longrunning.dao.OperationCompletedException;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.NotFoundException;
import io.grpc.Status;
import jakarta.annotation.Nullable;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.function.Supplier;

import static ai.lzy.model.db.DbHelper.withRetries;

public final class AllocateVmAction extends OperationRunnerBase {

    private static final Duration WAIT_VM_PERIOD = Duration.ofMillis(500);
    private static final Duration WAIT_VM_POLL_START = Duration.ofSeconds(3);
    private static final Duration WAIT_VM_POLL_PERIOD = Duration.ofSeconds(10);

    private Vm vm;
    private final AllocationContext allocationContext;
    private String tunnelPodName = null;
    private boolean kuberRequestDone = false;
    @Nullable
    private DeleteVmAction deleteVmAction = null;
    @Nullable
    private Instant vmLastPollTimestamp = null;

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
        return List.of(this::allocateTunnel, this::allocateVm, this::waitVm);
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
    protected void notifyFinished(@Nullable Throwable t) {
        allocationContext.metrics().runningAllocations.labels(vm.poolLabel()).dec();

        if (deleteVmAction != null) {
            log().info("{} Submit DeleteVmAction operation {}", logPrefix(), deleteVmAction.id());
            allocationContext.startNew(deleteVmAction);
            deleteVmAction = null;
        }
    }

    @Override
    protected void onExpired(@Nullable TransactionHandle tx) throws SQLException {
        prepareDeleteVmAction("Allocation op '%s' expired".formatted(vm.allocateState().operationId()), tx);
    }

    @Override
    protected void onCompletedOutside(Operation op, @Nullable TransactionHandle tx) throws SQLException {
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
                return tryFail(Status.INVALID_ARGUMENT.withDescription(e.getMessage()));
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
                    yield StepResult.CONTINUE;
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
            return tryFail(status);
        }
    }

    private StepResult waitVm() {
        log().info("{} ... waiting ...", logPrefix());

        var now = Instant.now();

        if (vmLastPollTimestamp == null) {
            vmLastPollTimestamp = now.plus(WAIT_VM_POLL_START);
            return StepResult.RESTART.after(WAIT_VM_PERIOD);
        }

        if (now.isAfter(vmLastPollTimestamp.plus(WAIT_VM_POLL_PERIOD))) {
            try {
                final var result = allocationContext.allocator().getVmAllocationStatus(vm);
                if (result.code() != VmAllocator.Result.Code.SUCCESS) {
                    return tryFail(Status.INTERNAL.withDescription("Wait VM failed: " + result.message()));
                }
            } catch (Exception e) {
                log().error("{} Error during allocation VM status checking: {}", logPrefix(), e.getMessage(), e);
            }
            vmLastPollTimestamp = now;
        }
        return StepResult.RESTART.after(WAIT_VM_PERIOD);
    }

    private void prepareDeleteVmAction(@Nullable String description, @Nullable TransactionHandle tx)
        throws SQLException
    {
        if (deleteVmAction != null) {
            return;
        }

        deleteVmAction = allocationContext.createDeleteVmAction(vm, description != null ? description : "",
            vm.allocateState().reqid(), tx);
    }

    private StepResult tryFail(Status status) {
        try {
            fail(status);
            return StepResult.FINISH;
        } catch (OperationCompletedException ex) {
            log().error("{} Cannot fail operation: already completed", logPrefix());
            return StepResult.FINISH;
        } catch (NotFoundException ex) {
            log().error("{} Cannot fail operation: not found", logPrefix());
            return StepResult.FINISH;
        } catch (Exception e) {
            log().error("{} Cannot fail operation: {}", logPrefix(), e.getMessage(), e);
            return StepResult.RESTART;
        }
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
