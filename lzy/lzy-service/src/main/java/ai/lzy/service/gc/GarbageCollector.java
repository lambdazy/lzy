package ai.lzy.service.gc;

import ai.lzy.model.db.DbHelper;
import ai.lzy.service.CleanExecutionCompanion;
import ai.lzy.service.config.LzyServiceConfig;
import ai.lzy.service.dao.ExecutionDao;
import ai.lzy.service.dao.GcDao;
import jakarta.annotation.PreDestroy;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;


public class GarbageCollector extends TimerTask {
    private static final Logger LOG = LogManager.getLogger(GarbageCollector.class);
    private static final long MIN_JITTER_PERIOD = 10;
    private static final long MAX_JITTER_PERIOD = 100;
    private final LzyServiceConfig config;
    private final GcDao gcDao;
    private final ExecutionDao executionDao;

    private final CleanExecutionCompanion cleanExecutionCompanion;


    private final String id;
    private final Timer timer = new Timer("gc-workflow-timer", true);
    private Timer taskTimer = null;

    private final long period;

    public GarbageCollector(LzyServiceConfig config, ExecutionDao executionDao,
                            GcDao gcDao, CleanExecutionCompanion cleanExecutionCompanion)
    {
        this.config = config;
        this.gcDao = gcDao;
        this.executionDao = executionDao;
        this.cleanExecutionCompanion = cleanExecutionCompanion;

        this.id = UUID.randomUUID().toString();

        this.period = config.getGcLeaderPeriod().toMillis() +
            (long) ((MAX_JITTER_PERIOD - MIN_JITTER_PERIOD) * Math.random() + MIN_JITTER_PERIOD);
    }

    public void start() {
        timer.scheduleAtFixedRate(this, period, period);
    }

    @Override
    public void run() {
        Timestamp now = Timestamp.from(Instant.now());
        Timestamp validUntil = Timestamp.from(now.toInstant()
            .plusMillis(config.getGcLeaderPeriod().toMillis()));

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

        long taskPeriod = config.getGcPeriod().toMillis();
        taskTimer = new Timer("gc-workflow-task-timer", true);
        taskTimer.scheduleAtFixedRate(new GarbageCollectorTask(id, executionDao, cleanExecutionCompanion),
            taskPeriod, taskPeriod);
        long markPeriod = config.getGcLeaderPeriod().toMillis() / 2;
        taskTimer.scheduleAtFixedRate(new MarkGcValid(config.getGcLeaderPeriod()), markPeriod, markPeriod);
    }

    @PreDestroy
    public void shutdown() {
        LOG.info("Shutdown GC {}", id);
        timer.cancel();
        clearTasks();
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
