package ai.lzy.allocator.gc;

import ai.lzy.allocator.AllocatorMain;
import ai.lzy.allocator.alloc.VmAllocator;
import ai.lzy.allocator.alloc.dao.VmDao;
import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.gc.dao.GcDao;
import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.util.auth.credentials.RenewableJwt;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.PreDestroy;
import javax.inject.Named;

import static ai.lzy.util.grpc.ProtoConverter.toProto;
import static java.util.concurrent.TimeUnit.SECONDS;

@Singleton
public class GarbageCollector {
    private static final Logger LOG = LogManager.getLogger(GarbageCollector.class);

    private final String instanceId;
    private final ServiceConfig.GcConfig config;
    private final VmDao vmDao;
    private final GcDao gcDao;
    private final OperationDao operationsDao;
    private final VmAllocator allocator;
    private final SubjectServiceGrpcClient subjectClient;
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(r -> {
        var th = new Thread(r, "gc");
        th.setUncaughtExceptionHandler((t, e) -> LOG.error("Uncaught exception in thread {}", t.getName(), e));
        return th;
    });
    private final AtomicReference<Instant> leaderDeadline = new AtomicReference<>(null);

    public GarbageCollector(ServiceConfig serviceConfig, ServiceConfig.GcConfig gcConfig, VmDao dao, GcDao gcDao,
                            @Named("AllocatorOperationDao") OperationDao operationDao, VmAllocator allocator,
                            @Named("AllocatorIamGrpcChannel") ManagedChannel iamChannel,
                            @Named("AllocatorIamToken") RenewableJwt iamToken)
    {
        this.instanceId = serviceConfig.getInstanceId();
        this.config = gcConfig;
        this.vmDao = dao;
        this.gcDao = gcDao;
        this.operationsDao = operationDao;
        this.allocator = allocator;
        this.subjectClient = new SubjectServiceGrpcClient(AllocatorMain.APP, iamChannel, iamToken::get);
    }

    public void start() {
        executor.scheduleAtFixedRate(new BecomeLeader(), 10, config.getLeaseDuration().getSeconds() / 3, SECONDS);
    }

    @PreDestroy
    public void shutdown() {
        LOG.info("Shutdown GC...");
        executor.shutdown();
        try {
            if (!executor.awaitTermination(1, TimeUnit.MINUTES)) {
                LOG.error("GC task was not completed in timeout");
            }
        } catch (InterruptedException e) {
            LOG.error("GC shutdown interrupted", e);
        }
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
            if (leaderDeadline.get() != null && leaderDeadline.get().isAfter(Instant.now())) {
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
                    schedule(new CleanVms(), Duration.ZERO);
                }
            } catch (Exception e) {
                LOG.error("Cannot acquire GC lease for {}: {}", instanceId, e.getMessage());
            }
        }
    }

    private class CleanVms implements Runnable {
        @Override
        public void run() {
            if (leaderDeadline.get() == null || leaderDeadline.get().isBefore(Instant.now())) {
                return;
            }

            try {
                var expiredVms = vmDao.listExpired(10);
                LOG.info("Found {} expired entries", expiredVms.size());

                expiredVms.forEach(vm -> {
                    try {
                        LOG.info("Clean VM {}", vm);

                        var allocOp = operationsDao.get(vm.allocOpId(), null);
                        if (allocOp != null && !allocOp.done()) {
                            LOG.info("Clean VM {}: try to fail allocation operation {}...", vm.vmId(), allocOp.id());
                            var status = toProto(Status.DEADLINE_EXCEEDED.withDescription("Vm is expired"));

                            var op = operationsDao.updateError(vm.allocOpId(), status.toByteArray(), null);
                            if (op == null) {
                                LOG.warn("Clean VM {}: allocate operation {} not found", vm.vmId(), vm.allocOpId());
                            }
                        }

                        var vmSubjectId = vm.allocateState().vmSubjectId();
                        if (vmSubjectId != null && !vmSubjectId.isEmpty()) {
                            LOG.info("Clean VM {}: removing IAM subject {}...", vm.vmId(), vmSubjectId);
                            subjectClient.removeSubject(new ai.lzy.iam.resources.subjects.Vm(vmSubjectId));
                        }

                        allocator.deallocate(vm.vmId());
                        //will retry deallocate if it fails
                        vmDao.deleteVm(vm.vmId());
                        LOG.info("Clean VM {}: done", vm.vmId());
                    } catch (Exception e) {
                        LOG.error("Error during clean up Vm {}", vm, e);
                    }
                });
            } catch (Exception e) {
                LOG.error("Error during GC: " + e.getMessage(), e);
            }

            schedule(this, config.getCleanupPeriod());
        }
    }
}
