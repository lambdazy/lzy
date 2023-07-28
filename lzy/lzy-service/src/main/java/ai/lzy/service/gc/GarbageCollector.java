package ai.lzy.service.gc;

import ai.lzy.model.db.DbHelper;
import ai.lzy.service.config.LzyServiceConfig;
import ai.lzy.service.dao.GcDao;
import ai.lzy.service.operations.OperationRunnersFactory;
import jakarta.annotation.Nullable;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;

@Singleton
public class GarbageCollector extends TimerTask {
    private static final Logger LOG = LogManager.getLogger(GarbageCollector.class);
    private static final long MIN_JITTER_PERIOD = 10;
    private static final long MAX_JITTER_PERIOD = 100;

    private final LzyServiceConfig config;
    private final GcDao gcDao;
    private final String id;
    private final Timer timer = new Timer("gc-workflow-timer", true);
    private Timer taskTimer = null;
    private final long period;
    private final OperationRunnersFactory operationRunnersFactory;
    @Nullable
    private volatile GarbageCollectorInterceptor interceptor = null;
    private final AtomicBoolean started = new AtomicBoolean(false);

    public GarbageCollector(LzyServiceConfig config, GcDao gcDao, OperationRunnersFactory operationRunnersFactory) {
        this.config = config;
        this.gcDao = gcDao;

        this.id = "LzyServiceGc-" + config.getInstanceId();

        this.period = config.getGc().getGcLeaderPeriod().toMillis() +
            (long) ((MAX_JITTER_PERIOD - MIN_JITTER_PERIOD) * Math.random() + MIN_JITTER_PERIOD);
        this.operationRunnersFactory = operationRunnersFactory;
    }

    public void start() {
        if (started.compareAndSet(false, true)) {
            timer.scheduleAtFixedRate(this, period, period);
        }
    }

    public void setInterceptor(@Nullable GarbageCollectorInterceptor interceptor) {
        this.interceptor = interceptor;
    }

    @Override
    public void run() {
        Timestamp now = Timestamp.from(Instant.now());
        Timestamp validUntil = Timestamp.from(now.toInstant()
            .plusMillis(config.getGc().getGcLeaderPeriod().toMillis()));

        try {
            var res = DbHelper.withRetries(LOG, () -> gcDao.updateGC(id, now, validUntil, null));
            if (!res) {
                LOG.debug("GC {} already leader", id);
                return;
            }
        } catch (Exception e) {
            LOG.debug("GC {} can't become leader", id);
            clearTasks();
            return;
        }

        LOG.info("GC {} became leader", id);

        long taskPeriod = config.getGc().getGcPeriod().toMillis();
        taskTimer = new Timer("gc-workflow-task-timer", true);
        taskTimer.scheduleAtFixedRate(
            new GarbageCollectorTask(id, interceptor, operationRunnersFactory), taskPeriod, taskPeriod);
        long markPeriod = config.getGc().getGcLeaderPeriod().toMillis() / 2;
        taskTimer.scheduleAtFixedRate(new MarkGcValid(config.getGc().getGcLeaderPeriod()), markPeriod, markPeriod);
    }

    @PreDestroy
    public void shutdown() {
        if (started.compareAndSet(true, false)) {
            LOG.info("Shutdown GC {}", id);
            timer.cancel();
            clearTasks();
        }
    }

    private void clearTasks() {
        if (taskTimer != null) {
            taskTimer.cancel();
            taskTimer = null;
        }
    }

    private class MarkGcValid extends TimerTask {
        private final Duration validPeriod;

        private MarkGcValid(Duration validPeriod) {
            this.validPeriod = validPeriod;
        }

        @Override
        public void run() {
            Timestamp now = Timestamp.from(Instant.now());
            Timestamp validUntil = Timestamp.from(now.toInstant().plusMillis(validPeriod.toMillis()));

            try {
                if (!gcDao.markGCValid(id, now, validUntil, null)) {
                    LOG.info("GC {} is not valid", id);
                    clearTasks();
                }
            } catch (Exception e) {
                LOG.info("GC {} can not update leadership", id, e);
                clearTasks();
            }
        }
    }
}
