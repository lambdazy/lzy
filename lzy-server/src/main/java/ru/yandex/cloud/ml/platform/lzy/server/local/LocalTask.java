package ru.yandex.cloud.ml.platform.lzy.server.local;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.Zygote;
import ru.yandex.cloud.ml.platform.lzy.model.utils.FreePortFinder;
import ru.yandex.cloud.ml.platform.lzy.server.ChannelsManager;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotMeta;

import java.net.URI;
import java.util.Map;
import java.util.UUID;

public abstract class LocalTask extends BaseTask {
    private static final Logger LOG = LogManager.getLogger(LocalTask.class);

    LocalTask(String owner, UUID tid, Zygote workload, Map<Slot, String> assignments,
              SnapshotMeta meta, ChannelsManager channels, URI serverURI) {
        super(owner, tid, workload, assignments, meta, channels, serverURI);
    }

    @Override
    public void start(String token) {
        final int port = FreePortFinder.find(10000, 20000);
        runServantAndWaitFor(serverURI.getHost(), serverURI.getPort(), "localhost", port, tid, token);
        LOG.info("LocalTask servant exited");
        state(State.DESTROYED);
    }
    @SuppressWarnings("SameParameterValue")
    protected abstract void runServantAndWaitFor(String serverHost, int serverPort, String servantHost, int servantPort, UUID tid, String token);
}
