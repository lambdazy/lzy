package ai.lzy.allocator;

import ai.lzy.allocator.alloc.AllocatorMetrics;
import ai.lzy.allocator.alloc.dao.SessionDao;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Singleton
public class DbAwareMetrics {
    private static final Logger LOG = LogManager.getLogger(DbAwareMetrics.class);

    private final SessionDao sessionDao;
    private final AllocatorMetrics allocatorMetrics;
    // guarded by this
    private ScheduledExecutorService executor = null;

    @Inject
    public DbAwareMetrics(SessionDao sessionDao, AllocatorMetrics allocatorMetrics) {
        this.sessionDao = sessionDao;
        this.allocatorMetrics = allocatorMetrics;
    }

    public synchronized void start() {
        if (executor != null) {
            return;
        }

        executor = Executors.newSingleThreadScheduledExecutor(r -> {
            var thread = new Thread(r, "db-aware-metrics");
            thread.setUncaughtExceptionHandler((t, e) -> LOG.error("Caught unexpected exception {} in thread {}: {}",
                e.getClass().getSimpleName(), t.getName(), e.getMessage(), e));
            return thread;
        });
        executor.scheduleWithFixedDelay(new Worker(), 0, 10, TimeUnit.SECONDS);
    }

    @PreDestroy
    public synchronized void shutdown() {
        executor.shutdownNow();
        executor = null;
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
