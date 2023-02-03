package ai.lzy.allocator.alloc;

import ai.lzy.allocator.exceptions.InvalidConfigurationException;
import ai.lzy.allocator.model.Vm;
import ai.lzy.allocator.model.debug.InjectedFailures;
import ai.lzy.iam.resources.credentials.SubjectCredentials;
import ai.lzy.iam.resources.subjects.AuthProvider;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.longrunning.Operation;
import ai.lzy.longrunning.OperationRunnerBase;
import ai.lzy.longrunning.dao.OperationCompletedException;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.NotFoundException;
import ai.lzy.util.auth.exceptions.AuthException;
import ai.lzy.util.auth.exceptions.AuthPermissionDeniedException;
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
        return List.of(this::createIamSubject, this::allocateTunnel, this::allocateVm, this::waitVm);
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
            allocationContext.submit(deleteVmAction);
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

    private StepResult createIamSubject() {
        InjectedFailures.failAllocateVm1();

        if (vm.instanceProperties().vmSubjectId() != null) {
            return StepResult.ALREADY_DONE;
        }

        var ottDeadline = vm.allocateState().startedAt().plus(Duration.ofMinutes(30));
        Subject vmSubj;
        try {
            vmSubj = allocationContext.subjectClient()
                .withIdempotencyKey(vm.vmId())
                .createSubject(
                    AuthProvider.INTERNAL,
                    vm.vmId(),
                    SubjectType.VM,
                    SubjectCredentials.ott("main", vm.allocateState().vmOtt(), ottDeadline));
            log().info("{} IAM subject {} created", logPrefix(), vmSubj.id());
        } catch (AuthException e) {
            if (e instanceof AuthPermissionDeniedException) {
                try {
                    fail(e.status().withDescription("Cannot create IAM subject for VM " + vm.vmId()));
                } catch (Exception ex) {
                    log().error("{} Cannot fail operation: {}", logPrefix(), ex.getMessage());
                    return StepResult.RESTART;
                }
                return StepResult.FINISH;
            } else {
                log().error("{} Cannot create IAM subject: {}. Retry later...", logPrefix(), e.getMessage());
                return StepResult.RESTART;
            }
        }

        InjectedFailures.failAllocateVm2();

        try {
            withRetries(log(), () -> allocationContext.vmDao().setVmSubjectId(vm.vmId(), vmSubj.id(), null));
            vm = vm.withVmSubjId(vmSubj.id());
        } catch (Exception e) {
            log().error("{} Cannot save IAM subject {}: {}. Retry later...", logPrefix(), vmSubj.id(), e.getMessage());
            return StepResult.RESTART;
        }

        return StepResult.CONTINUE;
    }

    private StepResult allocateTunnel() {
        InjectedFailures.failAllocateVm3();

        if (vm.proxyV6Address() == null) {
            return StepResult.ALREADY_DONE;
        }

        if (vm.instanceProperties().tunnelPodName() != null) {
            log().info("{} Found existing tunnel pod {} with address {} for VM",
                logPrefix(), vm.instanceProperties().tunnelPodName(), vm.proxyV6Address());
            return StepResult.ALREADY_DONE;
        }

        if (tunnelPodName == null) {
            try {
                tunnelPodName = allocationContext.tunnelAllocator().allocateTunnel(vm.spec());
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

    private StepResult waitVm() {
        log().info("{} ... waiting ...", logPrefix());
        return StepResult.RESTART.after(Duration.ofMillis(500));
    }

    private void prepareDeleteVmAction(String description, TransactionHandle tx) throws SQLException {
        if (deleteVmAction != null) {
            return;
        }

        deleteVmAction = allocationContext.createDeleteVmAction(vm, description, tx);
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
