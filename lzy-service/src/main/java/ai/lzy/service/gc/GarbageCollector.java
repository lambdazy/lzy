package ai.lzy.service.gc;

import ai.lzy.model.db.Storage;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.service.config.LzyServiceConfig;
import ai.lzy.service.data.dao.GcDao;
import ai.lzy.service.data.dao.WorkflowDao;
import ai.lzy.service.data.storage.LzyServiceStorage;
import ai.lzy.v1.AllocatorGrpc;
import ai.lzy.v1.channel.LzyChannelManagerPrivateGrpc;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;
import java.sql.Timestamp;
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
    private final Storage storage;
    private final AllocatorGrpc.AllocatorBlockingStub allocatorClient;
    private final LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateBlockingStub channelManagerClient;


    private final String id;
    private final Timer timer = new Timer("gc-workflow-timer", true);

    public GarbageCollector(LzyServiceConfig config, GcDao gcDao, WorkflowDao workflowDao, LzyServiceStorage storage,
                            AllocatorGrpc.AllocatorBlockingStub allocatorClient,
                            LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateBlockingStub channelManagerClient)
    {
        this.config = config;
        this.gcDao = gcDao;
        this.workflowDao = workflowDao;
        this.storage = storage;
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
            Timestamp lastUpdated = gcDao.getLastUpdated();
            Timestamp twoCyclesBefore = Timestamp.from(Instant.now().minusMillis(2 * config.getGcPeriod().toMillis()));

            if (lastUpdated == null || lastUpdated.before(twoCyclesBefore)) {
                try (var tr = TransactionHandle.create(storage)) {
                    gcDao.insertNewGcSession(tr, id);
                    tr.commit();
                }

                LOG.debug("GC {} became leader", id);

                long period = config.getGcPeriod().toMillis();
                timer.scheduleAtFixedRate(new GarbageCollectorTask(id, gcDao, workflowDao,
                    storage, allocatorClient, channelManagerClient), period, period);
            }
        } catch (SQLException e) {
            LOG.debug("GC {} can't become leader", id);
        }
    }

    public void shutdown() {
        LOG.info("Shutdown GC {}", id);
        timer.cancel();
    }
}
