package ai.lzy.allocator.alloc;

import ai.lzy.allocator.alloc.dao.VmDao;
import ai.lzy.allocator.alloc.impl.kuber.TunnelAllocator;
import ai.lzy.allocator.exceptions.InvalidConfigurationException;
import ai.lzy.allocator.model.Vm;
import ai.lzy.allocator.model.debug.InjectedFailures;
import ai.lzy.allocator.storage.AllocatorDataSource;
import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.iam.resources.credentials.SubjectCredentials;
import ai.lzy.iam.resources.subjects.AuthProvider;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.longrunning.OperationRunnerBase;
import ai.lzy.longrunning.dao.OperationCompletedException;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.NotFoundException;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

import static ai.lzy.model.db.DbHelper.withRetries;

public final class AllocateVmAction extends OperationRunnerBase {
    private Vm vm;
    private final AllocatorDataSource storage;
    private final VmDao vmDao;
    private final SubjectServiceGrpcClient subjectClient;
    private final VmAllocator allocator;
    private final TunnelAllocator tunnelAllocator;
    private final AllocatorMetrics metrics;
    private String tunnelPodName = null;

    public AllocateVmAction(Vm vm, AllocatorDataSource storage, OperationDao operationsDao, VmDao vmDao,
                            ScheduledExecutorService executor, SubjectServiceGrpcClient subjectClient,
                            VmAllocator allocator, TunnelAllocator tunnelAllocator, AllocatorMetrics metrics,
                            boolean restore)
    {
        super(vm.allocOpId(), "VM " + vm.vmId(), storage, operationsDao, executor);
        
        this.vm = vm;
        this.storage = storage;
        this.vmDao = vmDao;
        this.subjectClient = subjectClient;
        this.allocator = allocator;
        this.tunnelAllocator = tunnelAllocator;
        this.metrics = metrics;

        if (restore) {
            log().info("Restore VM {} allocation...", vm.toString());
        }
    }

    @Override
    protected List<Supplier<OperationRunnerBase.StepResult>> steps() {
        return List.of(this::createIamSubject, this::allocateTunnel, this::allocateVm);
    }

    @Override
    protected boolean isInjectedError(Error e) {
        return e instanceof InjectedFailures.TerminateException;
    }

    @Override
    protected void notifyExpired() {
        metrics.allocationError.inc();
        metrics.allocationTimeout.inc();
    }

    @Override
    protected void onExpired(TransactionHandle tx) throws SQLException  {
        vmDao.setStatus(vm.vmId(), Vm.Status.DELETING, tx);
    }

    private StepResult createIamSubject() {
        InjectedFailures.failAllocateVm1();

        if (vm.allocateState().vmSubjectId() != null) {
            return StepResult.CONTINUE;
        }

        var ottDeadline = vm.allocateState().startedAt().plus(Duration.ofMinutes(30));
        Subject vmSubj;
        try {
            vmSubj = subjectClient
                .withIdempotencyKey(vm.vmId())
                .createSubject(
                    AuthProvider.INTERNAL,
                    vm.vmId(),
                    SubjectType.VM,
                    SubjectCredentials.ott("main", vm.allocateState().vmOtt(), ottDeadline));
            log().info("Create VM {} IAM subject {} with OTT credentials", vm.vmId(), vmSubj.id());
        } catch (StatusRuntimeException e) {
            log().error("Cannot create IAM subject for VM {}: {}. Retry later...", vm.vmId(), e.getMessage());
            return StepResult.RESTART;
        }

        InjectedFailures.failAllocateVm2();

        try {
            withRetries(log(), () -> vmDao.setVmSubjectId(vm.vmId(), vmSubj.id(), null));
            vm = vm.withVmSubjId(vmSubj.id());
        } catch (Exception e) {
            log().error("Cannot save IAM subject {} for VM {}: {}. Retry later...",
                vmSubj.id(), vm.vmId(), e.getMessage());
            return StepResult.RESTART;
        }

        return updateOperationProgress();
    }

    private StepResult allocateTunnel() {
        InjectedFailures.failAllocateVm3();

        if (vm.proxyV6Address() == null) {
            return StepResult.CONTINUE;
        }

        if (vm.allocateState().tunnelPodName() != null) {
            log().info("Found existing tunnel pod {} with address {} for VM {}",
                vm.allocateState().tunnelPodName(), vm.proxyV6Address(), vm.vmId());
            return StepResult.CONTINUE;
        }

        if (tunnelPodName == null) {
            try {
                tunnelPodName = tunnelAllocator.allocateTunnel(vm.spec());
            } catch (Exception e) {
                metrics.allocationError.inc();
                log().error("Cannot allocate tunnel for VM {}: {}", vm.vmId(), e.getMessage());
                try {
                    withRetries(log(), () -> {
                        try (var tx = TransactionHandle.create(storage)) {
                            failOperation(Status.INVALID_ARGUMENT.withDescription(e.getMessage()), tx);
                            vmDao.setStatus(vm.vmId(), Vm.Status.DELETING, tx);
                            tx.commit();
                        }
                    });
                    return StepResult.FINISH;
                } catch (OperationCompletedException ex) {
                    log().error("Cannot fail operation {} (VM {}): already completed", vm.allocOpId(), vm.vmId());
                    return StepResult.FINISH;
                } catch (NotFoundException ex) {
                    log().error("Cannot fail operation {} (VM {}): not found", vm.allocOpId(), vm.vmId());
                    return StepResult.FINISH;
                } catch (Exception ex) {
                    log().error("Cannot fail operation {} (VM {}): {}. Retry later...",
                        vm.allocOpId(), vm.vmId(), ex.getMessage());
                    return StepResult.RESTART;
                }
            }
        }

        InjectedFailures.failAllocateVm4();

        try {
            withRetries(log(), () -> vmDao.setTunnelPod(vm.vmId(), tunnelPodName, null));
            vm = vm.withTunnelPod(tunnelPodName);
        } catch (Exception e) {
            log().error("Cannot save tunnel pod name {} for VM {}: {}. Retry later...",
                tunnelPodName, vm.vmId(), e.getMessage());
            return StepResult.RESTART;
        }

        return updateOperationProgress();
    }

    private StepResult allocateVm() {
        InjectedFailures.failAllocateVm5();

        try {
            var allocateStarted = allocator.allocate(vm);
            if (!allocateStarted) {
                return StepResult.RESTART;
            }
            return StepResult.FINISH;
        } catch (Exception e) {
            log().error("Error during VM {} allocation: {}", vm.vmId(), e.getMessage(), e);
            metrics.allocationError.inc();
            var status = e instanceof InvalidConfigurationException
                ? Status.INVALID_ARGUMENT.withDescription(e.getMessage())
                : Status.INTERNAL;
            try {
                withRetries(log(), () -> {
                    try (var tx = TransactionHandle.create(storage)) {
                        failOperation(status, tx);
                        vmDao.setStatus(vm.vmId(), Vm.Status.DELETING, tx);
                        tx.commit();
                    }
                });
                return StepResult.FINISH;
            } catch (OperationCompletedException ex) {
                log().error("Cannot fail operation {} (VM {}): already completed", vm.allocOpId(), vm.vmId());
                return StepResult.FINISH;
            } catch (NotFoundException ex) {
                log().error("Cannot fail operation {} (VM {}): not found", vm.allocOpId(), vm.vmId());
                return StepResult.FINISH;
            } catch (Exception ex) {
                log().error("Cannot fail operation {} (VM {}): {}", vm.allocOpId(), vm.vmId(), e.getMessage());
                return StepResult.RESTART;
            }
        }
    }
}
