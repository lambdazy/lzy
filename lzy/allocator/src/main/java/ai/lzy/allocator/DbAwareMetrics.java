package ai.lzy.allocator;

import ai.lzy.allocator.alloc.AllocatorMetrics;
import ai.lzy.allocator.alloc.dao.SessionDao;
import ai.lzy.allocator.alloc.dao.VmDao;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

// TODO: do it on GC leader only
@Singleton
public class DbAwareMetrics {
    private static final Logger LOG = LogManager.getLogger(DbAwareMetrics.class);

    private final SessionDao sessionDao;
    private final VmDao vmDao;
    private final AllocatorMetrics allocatorMetrics;
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(r -> {
        var thread = new Thread(r, "db-aware-metrics");
        thread.setUncaughtExceptionHandler((t, e) -> LOG.error("Caught unexpected exception {} in thread {}: {}",
            e.getClass().getSimpleName(), t.getName(), e.getMessage(), e));
        return thread;
    });

    @Inject
    public DbAwareMetrics(SessionDao sessionDao, VmDao vmDao, AllocatorMetrics allocatorMetrics) {
        this.sessionDao = sessionDao;
        this.vmDao = vmDao;
        this.allocatorMetrics = allocatorMetrics;

        executor.scheduleWithFixedDelay(new Worker(), 1, 10, TimeUnit.SECONDS);
    }

    @PreDestroy
    public void shutdown() {
        executor.shutdownNow();
    }

    private class Worker implements Runnable {
        @Override
        public void run() {
            reportActiveSessions();

        }

        private void reportActiveSessions() {
            try {
                allocatorMetrics.activeSessions.set(sessionDao.countActiveSessions());
            } catch (Throwable e) {
                LOG.error("Cannot count active sessions: {}", e.getMessage());
            }
        }
    }
}
