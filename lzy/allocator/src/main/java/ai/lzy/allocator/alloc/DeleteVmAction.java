package ai.lzy.allocator.alloc;

import ai.lzy.allocator.model.Vm;
import ai.lzy.allocator.model.debug.InjectedFailures;
import ai.lzy.iam.resources.subjects.AuthProvider;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.longrunning.OperationRunnerBase;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.util.auth.exceptions.AuthException;
import ai.lzy.util.auth.exceptions.AuthNotFoundException;
import com.google.protobuf.Any;
import com.google.protobuf.Empty;

import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.function.Supplier;

import static ai.lzy.model.db.DbHelper.withRetries;

public final class DeleteVmAction extends OperationRunnerBase {
    private final String vmId;
    private final AllocationContext allocationContext;
    private Vm vm = null;
    private String vmSubjectId = null;
    private boolean iamSubjectDeleted = false;
    private boolean tunnelDeleted = false;
    private boolean deallocated = false;

    public DeleteVmAction(String vmId, String deleteOpId, AllocationContext allocationContext) {
        super(deleteOpId, "VM " + vmId, allocationContext.storage(), allocationContext.operationsDao(),
            allocationContext.executor());

        this.vmId = vmId;
        this.allocationContext = allocationContext;

        log().info("{} Delete VM...", logPrefix());
    }

    public DeleteVmAction(Vm vm, String deleteOpId, AllocationContext allocationContext) {
        this(vm.vmId(), deleteOpId, allocationContext);
        this.vm = vm;
    }

    @Override
    protected boolean isInjectedError(Error e) {
        return e instanceof InjectedFailures.TerminateException;
    }

    @Override
    protected void notifyExpired() {
    }

    @Override
    protected void notifyFinished() {
    }

    @Override
    protected void onExpired(TransactionHandle tx) {
        throw new RuntimeException("Unexpected, vm: '%s', op: '%s'".formatted(vmId, id()));
    }

    @Override
    protected List<Supplier<StepResult>> steps() {
        return List.of(this::start, this::deleteIamSubject, this::deleteTunnel, this::deallocateVm, this::cleanDb);
    }

    private StepResult start() {
        if (vm != null) {
            return StepResult.ALREADY_DONE;
        }

        try {
            vm = withRetries(log(), () -> allocationContext.vmDao().get(vmId, null));
            if (vm == null) {
                log().error("{}: VM doesn't exist", logPrefix());
                return StepResult.FINISH;
            }

        } catch (Exception e) {
            log().error("{} Cannot load VM: {}, Reschedule...", logPrefix(), e.getMessage());
            return StepResult.RESTART;
        }

        return switch (vm.status()) {
            case DELETING -> StepResult.CONTINUE;
            case RUNNING, IDLE -> {
                if (vm.runState().activityDeadline().isBefore(Instant.now())) {
                    // do nothing for expired Vm
                    yield StepResult.CONTINUE;
                }

                // TODO: some logic of communicating with VM's allocator agent should be here
                // for Lzy: ensure all slots are flushed
                yield StepResult.CONTINUE;
            }
            case ALLOCATING -> throw new IllegalStateException("Unexpected value: " + vm.status());
        };
    }

    private StepResult deleteIamSubject() {
        if (iamSubjectDeleted) {
            return StepResult.ALREADY_DONE;
        }

        if (vmSubjectId == null) {
            vmSubjectId = vm.instanceProperties().vmSubjectId();
            if (vmSubjectId == null || vmSubjectId.isEmpty()) {
                try {
                    var vmSubject = allocationContext.subjectClient()
                        .findSubject(AuthProvider.INTERNAL, vm.vmId(), SubjectType.VM);
                    if (vmSubject != null) {
                        log().error("{}: Found leaked IAM subject {}", logPrefix(), vmSubject.id());
                        vmSubjectId = vmSubject.id();
                    }
                } catch (AuthException e) {
                    log().info("{} Removing IAM subject {} error: {}", logPrefix(), vmSubjectId, e.getMessage());
                }
            }
        }

        if (vmSubjectId != null && !vmSubjectId.isEmpty()) {
            log().info("{} Removing IAM subject {}...", logPrefix(), vmSubjectId);
            try {
                allocationContext.subjectClient().removeSubject(new ai.lzy.iam.resources.subjects.Vm(vmSubjectId));
                iamSubjectDeleted = true;
            } catch (AuthException e) {
                if (e instanceof AuthNotFoundException) {
                    log().warn("{} IAM subject {} not found", logPrefix(), vmSubjectId);
                    iamSubjectDeleted = true;
                } else {
                    log().error("{} Error during deleting IAM subject {}: {}",
                        logPrefix(), vmSubjectId, e.getMessage());
                }
            }
        }

        return StepResult.CONTINUE;
    }

    private StepResult deleteTunnel() {
        var name = vm.instanceProperties().tunnelPodName();
        if (name == null) {
            return StepResult.ALREADY_DONE;
        }

        if (tunnelDeleted) {
            return StepResult.ALREADY_DONE;
        }

        try {
            allocationContext.tunnelAllocator().deallocateTunnel(name);
            tunnelDeleted = true;
            return StepResult.CONTINUE;
        } catch (Exception e) {
            log().error("{} Cannot delete tunnel pod {}: {}", logPrefix(), name, e.getMessage(), e);
            return StepResult.RESTART;
        }
    }

    private StepResult deallocateVm() {
        if (deallocated) {
            return StepResult.ALREADY_DONE;
        }

        log().info("{} Trying to deallocate VM...", logPrefix());
        try {
            var ret = allocationContext.allocator().deallocate(vm);
            return switch (ret.code()) {
                case SUCCESS -> {
                    deallocated = true;
                    log().info("{} VM deallocated", logPrefix());
                    yield StepResult.CONTINUE;
                }
                case RETRY_LATER -> {
                    log().error("{} Cannot deallocate VM: {}. Retry later...", logPrefix(), ret.message());
                    yield StepResult.RESTART;
                }
                case FAILED -> {
                    log().error("{} Deallocation failed: {}", logPrefix(), ret.message());
                    yield StepResult.FINISH;
                }
            };
        } catch (Exception e) {
            log().error("{} Error while deallocating VM: {}", logPrefix(), e.getMessage(), e);
            return StepResult.RESTART;
        }
    }

    private StepResult cleanDb() {
        try (var tx = TransactionHandle.create(allocationContext.storage())) {
            completeOperation(null, Any.pack(Empty.getDefaultInstance()), tx);
            allocationContext.vmDao().cleanupVm(vm.vmId(), tx);

            // TODO: move to GC
            // allocationContext.operationsDao().deleteCompletedOperation(vm.allocateState().operationId(), tx);
            // allocationContext.operationsDao().deleteCompletedOperation(vm.deleteState().operationId(), tx);

            tx.commit();
        } catch (SQLException e) {
            log().error("{} Cannot cleanup DB after deleting VM: {}", logPrefix(), e.getMessage());
            return StepResult.RESTART;
        }
        log().info("{} VM deleted", logPrefix());
        return StepResult.FINISH;
    }
}
