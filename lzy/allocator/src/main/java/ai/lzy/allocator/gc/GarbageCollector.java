package ai.lzy.allocator.gc;

import ai.lzy.allocator.alloc.AllocatorMetrics;
import ai.lzy.allocator.alloc.VmAllocator;
import ai.lzy.allocator.alloc.dao.VmDao;
import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.gc.dao.GcDao;
import ai.lzy.allocator.model.Vm;
import ai.lzy.allocator.storage.AllocatorDataSource;
import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.iam.resources.subjects.AuthProvider;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.longrunning.dao.OperationDao;
import com.google.common.annotations.VisibleForTesting;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicReference;

import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.util.grpc.ProtoConverter.toProto;
import static java.util.concurrent.TimeUnit.SECONDS;

@Singleton
public class GarbageCollector {
    private static final Logger LOG = LogManager.getLogger(GarbageCollector.class);

    private final String instanceId;
    private final ServiceConfig.GcConfig config;
    private final AllocatorDataSource storage;
    private final VmDao vmDao;
    private final GcDao gcDao;
    private final OperationDao operationsDao;
    private final VmAllocator allocator;
    private final AllocatorMetrics allocatorMetrics;
    private final SubjectServiceGrpcClient subjectClient;
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(r -> {
        var th = new Thread(r, "gc");
        th.setUncaughtExceptionHandler((t, e) -> LOG.error("Uncaught exception in thread {}", t.getName(), e));
        return th;
    });
    private final AtomicReference<Instant> leaderDeadline = new AtomicReference<>(null);
    private volatile ScheduledFuture<?> becomeLeaderFuture;

    public GarbageCollector(ServiceConfig serviceConfig, ServiceConfig.GcConfig gcConfig, AllocatorDataSource storage,
                            VmDao dao, GcDao gcDao, @Named("AllocatorOperationDao") OperationDao operationDao,
                            VmAllocator allocator, AllocatorMetrics allocatorMetrics,
                            @Named("AllocatorSubjectServiceClient") SubjectServiceGrpcClient subjectClient)
    {
        this.instanceId = serviceConfig.getInstanceId();
        this.config = gcConfig;
        this.storage = storage;
        this.vmDao = dao;
        this.gcDao = gcDao;
        this.operationsDao = operationDao;
        this.allocator = allocator;
        this.allocatorMetrics = allocatorMetrics;
        this.subjectClient = subjectClient;
    }

    public void start() {
        becomeLeaderFuture = executor.scheduleAtFixedRate(
            new BecomeLeader(),
            config.getInitialDelay().getSeconds(),
            config.getLeaseDuration().getSeconds() / 3,
            SECONDS);
    }

    @PreDestroy
    public void shutdown() {
        LOG.info("Shutdown GC...");
        if (becomeLeaderFuture != null) {
            becomeLeaderFuture.cancel(true);
        }
        executor.shutdown();
        try {
            if (!executor.awaitTermination(config.getGracefulShutdownDuration().getSeconds(), SECONDS)) {
                LOG.error("GC task was not completed in timeout");
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            LOG.error("GC shutdown interrupted", e);
        }

        if (isLeader()) {
            LOG.info("Release GC lease...");
            try {
                gcDao.release(instanceId);
            } catch (SQLException e) {
                LOG.error("Cannot release GC lease for {}: {}", instanceId, e.getMessage());
            }
        }
    }

    @VisibleForTesting
    public void forceRun() {
        LOG.info("Force run GC...");
        new CleanVms(true).run();
    }

    private boolean isLeader() {
        return leaderDeadline.get() != null && leaderDeadline.get().isAfter(Instant.now());
    }

    private void schedule(Runnable task, Duration delay) {
        if (!executor.isShutdown()) {
            try {
                executor.schedule(task, delay.toSeconds(), SECONDS);
            } catch (RejectedExecutionException e) {
                if (!executor.isShutdown()) {
                    LOG.error("Cannot schedule GC task: {}", e.getMessage());
                }
            }
        }
    }

    private class BecomeLeader implements Runnable {
        @Override
        public void run() {
            if (isLeader()) {
                try {
                    var newDeadline = gcDao.prolongLeader(instanceId, config.getLeaseDuration());
                    leaderDeadline.set(newDeadline);
                    LOG.debug("GC lease for {} prolonged until {}", instanceId, newDeadline);
                } catch (Exception e) {
                    LOG.error("Cannot prolong GC lease for {}: {}", instanceId, e.getMessage());
                }
                return;
            }

            try {
                var newDeadline = gcDao.tryAcquire(instanceId, config.getLeaseDuration(), null);
                leaderDeadline.set(newDeadline);
                if (newDeadline != null) {
                    LOG.info("New GC leader {}", instanceId);
                    schedule(new CleanVms(false), Duration.ZERO);
                }
            } catch (Exception e) {
                LOG.error("Cannot acquire GC lease for {}: {}", instanceId, e.getMessage());
            }
        }
    }

    private class CleanVms implements Runnable {
        private final boolean force;

        private CleanVms(boolean force) {
            this.force = force;
        }

        @Override
        public void run() {
            if (!force && (leaderDeadline.get() == null || leaderDeadline.get().isBefore(Instant.now()))) {
                return;
            }

            var startTime = Instant.now();

            try {
                var failedVms = vmDao.findFailedVms(null);
                if (!failedVms.isEmpty()) {
                    LOG.info("Found {} failed VM allocations: [{}]", failedVms.size(), String.join(", ", failedVms));
                }
            } catch (Exception e) {
                LOG.error("Cannot find failed allocation operations: {}", e.getMessage());
            }

            try {
                var vms = vmDao.listVmsToClean(10);
                LOG.info("Found {} Vms to clean", vms.size());

                vms.forEach(this::cleanVm);
            } catch (Exception e) {
                LOG.error("Error during GC: " + e.getMessage(), e);
            }

            // TODO: cleanup disks

            LOG.info("GC step takes {}ms", Duration.between(startTime, Instant.now()).toMillis());

            schedule(this, config.getCleanupPeriod());
        }

        public void cleanVm(final Vm vm) {
            LOG.warn("About to delete VM {}", vm);

            try {
                var allocOp = operationsDao.get(vm.allocOpId(), null);
                if (allocOp != null && !allocOp.done()) {
                    LOG.info("Clean VM {}: try to fail allocation operation {}...", vm.vmId(), allocOp.id());
                    var status = toProto(Status.DEADLINE_EXCEEDED.withDescription("Vm is expired"));
                    operationsDao.fail(vm.allocOpId(), status, null);
                    return;
                }

                var vmSubjectId = vm.allocateState().vmSubjectId();
                if (vmSubjectId == null || vmSubjectId.isEmpty()) {
                    var vmSubject = subjectClient.findSubject(AuthProvider.INTERNAL, vm.vmId(), SubjectType.VM);
                    if (vmSubject != null) {
                        LOG.error("Clean VM {}: found leaked IAM subject {}", vm.vmId(), vmSubject.id());
                        vmSubjectId = vmSubject.id();
                    }

                }
                if (vmSubjectId != null && !vmSubjectId.isEmpty()) {
                    LOG.info("Clean VM {}: removing IAM subject {}...", vm.vmId(), vmSubjectId);
                    try {
                        subjectClient.removeSubject(new ai.lzy.iam.resources.subjects.Vm(vmSubjectId));
                    } catch (StatusRuntimeException e) {
                        if (e.getStatus().getCode().equals(Status.Code.NOT_FOUND)) {
                            LOG.warn("Clean VM {}: IAM subject {} not found", vm.vmId(), vmSubjectId);
                        } else {
                            LOG.error("Error during cleaning VM {}: {}", vm.vmId(), e.getMessage());
                            return;
                        }
                    }
                }

                // TODO: ensure all slots are flushed

                // will retry deallocate if it fails
                allocator.deallocate(vm.vmId());

                // TODO: delete tunnel if any

                withRetries(LOG, () -> vmDao.deleteVm(vm.vmId(), null));

                var done = switch (vm.status()) {
                    case ALLOCATING, DELETING -> {
                        // TODO: ...
                        yield 41;
                    }
                    case RUNNING -> {
                        allocatorMetrics.runningVms.labels(vm.poolLabel()).dec();
                        yield 42;
                    }
                    case IDLE -> {
                        allocatorMetrics.cachedVms.labels(vm.poolLabel()).dec();
                        allocatorMetrics.cachedVmsTime.labels(vm.poolLabel())
                            .inc(Duration.between(vm.idleState().idleSice(), Instant.now()).getSeconds());
                        yield 43;
                    }
                };

                unused(done);

                withRetries(LOG, () -> operationsDao.deleteCompletedOperation(vm.allocOpId(), null));

                LOG.info("Clean VM {}: done", vm.vmId());
            } catch (Exception e) {
                LOG.error("Error during clean up Vm {}", vm, e);
            }
        }
    }

    private static void unused(int ignored) {
    }
}
