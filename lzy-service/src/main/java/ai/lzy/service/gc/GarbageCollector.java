package ai.lzy.service.gc;

import ai.lzy.service.config.LzyServiceConfig;
import ai.lzy.service.data.dao.GcDao;
import ai.lzy.service.data.dao.WorkflowDao;
import ai.lzy.v1.AllocatorGrpc;
import ai.lzy.v1.channel.LzyChannelManagerPrivateGrpc;
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
    private static final long MIN_JITTER_PERIOD = 500;
    private static final long MAX_JITTER_PERIOD = 1000;
    private final LzyServiceConfig config;
    private final GcDao gcDao;
    private final WorkflowDao workflowDao;
    private final AllocatorGrpc.AllocatorBlockingStub allocatorClient;
    private final LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateBlockingStub channelManagerClient;


    private final String id;
    private final Timer timer = new Timer("gc-workflow-timer", true);
    private Timer taskTimer = null;

    public GarbageCollector(LzyServiceConfig config, GcDao gcDao, WorkflowDao workflowDao,
                            AllocatorGrpc.AllocatorBlockingStub allocatorClient,
                            LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateBlockingStub channelManagerClient)
    {
        this.config = config;
        this.gcDao = gcDao;
        this.workflowDao = workflowDao;
        this.allocatorClient = allocatorClient;
        this.channelManagerClient = channelManagerClient;

        this.id = UUID.randomUUID().toString();

        long period = config.getGcLeaderPeriod().toMillis() +
            (long) ((MAX_JITTER_PERIOD - MIN_JITTER_PERIOD) * Math.random() + MIN_JITTER_PERIOD);
        timer.scheduleAtFixedRate(this, period, period);
    }

    @Override
    public void run() {
        try {
            Timestamp now = Timestamp.from(Instant.now());
            Timestamp validUntil = Timestamp.from(now.toInstant()
                .plusMillis(config.getGcLeaderPeriod().toMillis() / 2));
            gcDao.updateGC(id, now, validUntil, null);

            LOG.debug("GC {} became leader", id);

            long taskPeriod = config.getGcPeriod().toMillis();
            taskTimer = new Timer("gc-workflow-task-timer", true);
            taskTimer.scheduleAtFixedRate(new GarbageCollectorTask(id, workflowDao,
                allocatorClient, channelManagerClient), taskPeriod, taskPeriod);
            long markPeriod = config.getGcLeaderPeriod().toMillis() / 2;
            taskTimer.scheduleAtFixedRate(new MarkGcValid(config.getGcLeaderPeriod()), markPeriod, markPeriod);
        } catch (Exception e) {
            LOG.debug("GC {} can't become leader", id);
            clearTasks();
        }
    }

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
            try {
                Timestamp now = Timestamp.from(Instant.now());
                Timestamp validUntil = Timestamp.from(now.toInstant().plusMillis(validPeriod.toMillis()));

                gcDao.markGcValid(id, now, validUntil, null);
            } catch (IllegalArgumentException e) {
                LOG.debug("GC {} already valid", id);
            } catch (Exception e) {
                LOG.info("GC {} can not update leadership", id);
                clearTasks();
            }
        }
    }
}
