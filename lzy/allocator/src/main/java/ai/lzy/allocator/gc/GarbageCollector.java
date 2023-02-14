package ai.lzy.allocator.gc;

import ai.lzy.allocator.alloc.AllocationContext;
import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.gc.dao.GcDao;
import ai.lzy.allocator.model.Vm;
import com.google.common.annotations.VisibleForTesting;
import jakarta.annotation.PreDestroy;
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
import java.util.concurrent.locks.LockSupport;

import static java.util.concurrent.TimeUnit.SECONDS;

@Singleton
public class GarbageCollector {
    private static final Logger LOG = LogManager.getLogger(GarbageCollector.class);

    private final String instanceId;
    private final ServiceConfig.GcConfig config;
    private final GcDao gcDao;
    private final AllocationContext allocationContext;
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(r -> {
        var th = new Thread(r, "gc");
        th.setUncaughtExceptionHandler((t, e) -> LOG.error("Uncaught exception in thread {}", t.getName(), e));
        return th;
    });
    private final AtomicReference<Instant> leaderDeadline = new AtomicReference<>(null);
    private volatile ScheduledFuture<?> becomeLeaderFuture;

    public GarbageCollector(ServiceConfig serviceConfig, ServiceConfig.GcConfig gcConfig, GcDao gcDao,
                            AllocationContext allocationContext)
    {
        this.instanceId = serviceConfig.getInstanceId();
        this.config = gcConfig;
        this.gcDao = gcDao;
        this.allocationContext = allocationContext;
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
                var vms = allocationContext.vmDao().listExpiredVms(10);
                LOG.info("Found {} Vms to clean", vms.size());
                var ops = vms.stream().map(this::cleanVm).toList();
                if (force) {
                    ops.forEach(opId -> {
                        if (opId == null) {
                            return;
                        }

                        try {
                            while (true) {
                                var op = allocationContext.operationsDao().get(opId, null);
                                if (op == null || op.done()) {
                                    return;
                                }
                                LockSupport.parkNanos(Duration.ofMillis(50).toNanos());
                            }
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                    });
                }
            } catch (Throwable e) {
                LOG.error("Error during GC: " + e.getMessage(), e);
            }

            // TODO: cleanup disks

            // TODO: cleanup completed ops

            LOG.info("GC step takes {}ms", Duration.between(startTime, Instant.now()).toMillis());

            schedule(this, config.getCleanupPeriod());
        }

        public String cleanVm(final Vm vm) {
            try {
                var reqid = "gc-expired-" + vm.vmId();
                return allocationContext.submitDeleteVmAction(vm, "Delete expired VM %s".formatted(vm), reqid, LOG);
            } catch (Exception e) {
                LOG.error("Cannot cleanup expired VM {}: {}", vm, e.getMessage());
                return null;
            }
        }
    }
}
