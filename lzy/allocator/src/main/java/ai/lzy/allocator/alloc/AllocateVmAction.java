package ai.lzy.allocator.alloc;

import ai.lzy.allocator.alloc.dao.VmDao;
import ai.lzy.allocator.alloc.exceptions.InvalidConfigurationException;
import ai.lzy.allocator.alloc.impl.kuber.TunnelAllocator;
import ai.lzy.allocator.model.Vm;
import ai.lzy.allocator.model.debug.InjectedFailures;
import ai.lzy.allocator.storage.AllocatorDataSource;
import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.iam.resources.credentials.SubjectCredentials;
import ai.lzy.iam.resources.subjects.AuthProvider;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.NotFoundException;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.util.grpc.ProtoConverter.toProto;

public final class AllocateVmAction implements Runnable {
    private static final Logger LOG = LogManager.getLogger(AllocateVmAction.class);
    
    private Vm vm;
    private final boolean restore;
    private final AllocatorDataSource storage;
    private final OperationDao operationsDao;
    private final VmDao vmDao;
    private final ScheduledExecutorService executor;
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
        this.vm = vm;
        this.storage = storage;
        this.operationsDao = operationsDao;
        this.vmDao = vmDao;
        this.executor = executor;
        this.subjectClient = subjectClient;
        this.allocator = allocator;
        this.tunnelAllocator = tunnelAllocator;
        this.metrics = metrics;
        this.restore = restore;
    }

    @Override
    public void run() {
        try {
            runImpl();
        } catch (InjectedFailures.TerminateException e) {
            LOG.error("Terminate action by InjectedFailure exception: {}", e.getMessage());
        }
    }

    private void runImpl() {
        if (restore) {
            LOG.info("Restore VM {} allocation...", vm.toString());
        }

        if (!validateOp()) {
            return;
        }

        if (vm.allocateState().deadline().isBefore(Instant.now())) {
            expireAllocation();
            return;
        }

        InjectedFailures.failAllocateVm1();

        if (vm.allocateState().vmSubjectId() == null) {
            if (!createIamSubject()) {
                return;
            }
        }

        InjectedFailures.failAllocateVm3();

        if (vm.proxyV6Address() != null) {
            if (!allocateTunnel()) {
                return;
            }
        }

        InjectedFailures.failAllocateVm5();

        try {
            var allocateStarted = allocator.allocate(vm);
            if (!allocateStarted) {
                executor.schedule(this, 1, TimeUnit.SECONDS);
            }
        } catch (Exception e) {
            LOG.error("Error during VM {} allocation: {}", vm.vmId(), e.getMessage(), e);
            metrics.allocationError.inc();
            var status = e instanceof InvalidConfigurationException
                ? Status.INVALID_ARGUMENT.withDescription(e.getMessage())
                : Status.INTERNAL;
            try {
                withRetries(LOG, () -> {
                    try (var tx = TransactionHandle.create(storage)) {
                        var op = operationsDao.fail(vm.allocOpId(), toProto(status), tx);
                        if (op == null) {
                            vmDao.setStatus(vm.vmId(), Vm.Status.DELETING, tx);
                            tx.commit();
                        }
                    }
                });
            } catch (Exception ex) {
                LOG.error("Cannot fail operation {} (VM {}): {}", vm.allocOpId(), vm.vmId(), e.getMessage());
                executor.schedule(this, 1, TimeUnit.SECONDS);
            }
        }
    }

    private boolean validateOp() {
        try {
            var op = withRetries(LOG, () -> operationsDao.get(vm.allocOpId(), null));
            if (op == null) {
                LOG.error("Operation {} (VM {}) not found", vm.allocOpId(), vm.vmId());
                return false;
            }
            if (op.done()) {
                if (op.response() != null) {
                    LOG.warn("Operation {} (VM {}) already successfully completed", vm.allocOpId(), vm.vmId());
                } else {
                    LOG.warn("Operation {} (VM {}) already completed with error: {}",
                        vm.allocOpId(), vm.vmId(), op.error());
                }
                return false;
            }
            return true;
        } catch (Exception e) {
            LOG.error("Cannot load operation {} (VM {}): {}. Retry later...",
                vm.allocOpId(), vm.vmId(), e.getMessage());
            executor.schedule(this, 1, TimeUnit.SECONDS);
            return false;
        }
    }

    private void expireAllocation() {
        LOG.warn("Allocation operation {} (VM {}) is expired", vm.allocOpId(), vm.vmId());
        metrics.allocationError.inc();
        metrics.allocationTimeout.inc();
        try {
            withRetries(LOG, () -> {
                try (var tx = TransactionHandle.create(storage)) {
                    var op = operationsDao.fail(vm.allocOpId(), toProto(Status.DEADLINE_EXCEEDED), tx);
                    if (op == null) {
                        vmDao.setStatus(vm.vmId(), Vm.Status.DELETING, tx);
                        tx.commit();
                    }
                }
            });
        } catch (NotFoundException e) {
            LOG.error("Cannot fail operation {} (VM {}): not found", vm.allocOpId(), vm.vmId());
        } catch (Exception e) {
            LOG.error("Cannot fail operation {} (VM {}): {}. Retry later...",
                vm.allocOpId(), vm.vmId(), e.getMessage());
            executor.schedule(this, 1, TimeUnit.SECONDS);
        }
    }

    private boolean createIamSubject() {
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
            LOG.info("Create VM {} IAM subject {} with OTT credentials", vm.vmId(), vmSubj.id());
        } catch (StatusRuntimeException e) {
            LOG.error("Cannot create IAM subject for VM {}: {}. Retry later...", vm.vmId(), e.getMessage());
            executor.schedule(this, 1, TimeUnit.SECONDS);
            return false;
        }

        InjectedFailures.failAllocateVm2();

        try {
            withRetries(LOG, () -> vmDao.setVmSubjectId(vm.vmId(), vmSubj.id(), null));
            vm = vm.withVmSubjId(vmSubj.id());
        } catch (Exception e) {
            LOG.error("Cannot save IAM subject {} for VM {}: {}. Retry later...",
                vmSubj.id(), vm.vmId(), e.getMessage());
            executor.schedule(this, 1, TimeUnit.SECONDS);
            return true;
        }

        return updateOperationProgress();
    }

    private boolean allocateTunnel() {
        if (vm.allocateState().tunnelPodName() != null) {
            LOG.info("Found existing tunnel pod {} with address {} for VM {}",
                vm.allocateState().tunnelPodName(), vm.proxyV6Address(), vm.vmId());
            return true;
        }

        if (tunnelPodName == null) {
            try {
                tunnelPodName = tunnelAllocator.allocateTunnel(vm.spec());
            } catch (Exception e) {
                metrics.allocationError.inc();
                LOG.error("Cannot allocate tunnel for VM {}: {}", vm.vmId(), e.getMessage());
                try {
                    withRetries(LOG, () -> {
                        try (var tx = TransactionHandle.create(storage)) {
                            var op = operationsDao.fail(
                                vm.allocOpId(),
                                toProto(Status.INVALID_ARGUMENT.withDescription(e.getMessage())),
                                tx);
                            if (op == null) {
                                vmDao.setStatus(vm.vmId(), Vm.Status.DELETING, tx);
                                tx.commit();
                            }
                        }
                    });
                } catch (Exception ex) {
                    LOG.error("Cannot fail operation {} (VM {}): {}. Retry later...",
                        vm.allocOpId(), vm.vmId(), ex.getMessage());
                    executor.schedule(this, 1, TimeUnit.SECONDS);
                }
                return false;
            }
        }

        InjectedFailures.failAllocateVm4();

        try {
            withRetries(LOG, () -> vmDao.setTunnelPod(vm.vmId(), tunnelPodName, null));
            vm = vm.withTunnelPod(tunnelPodName);
        } catch (Exception e) {
            LOG.error("Cannot save tunnel pod name {} for VM {}: {}. Retry later...",
                tunnelPodName, vm.vmId(), e.getMessage());
            executor.schedule(this, 1, TimeUnit.SECONDS);
            return false;
        }

        return updateOperationProgress();
    }

    private boolean updateOperationProgress() {
        try {
            var op = withRetries(LOG, () -> operationsDao.update(vm.allocOpId(), null));
            return op == null;
        } catch (NotFoundException e) {
            LOG.error("Cannot update operation {} (VM {}): not found", vm.allocOpId(), vm.vmId());
            return false;
        } catch (Exception e) {
            LOG.error("Cannot update operation {} (VM {}): {}", vm.allocOpId(), vm.vmId(), e.getMessage());
            executor.schedule(this, 1, TimeUnit.SECONDS);
            return false;
        }
    }
}
