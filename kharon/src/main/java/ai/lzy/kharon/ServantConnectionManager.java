package ai.lzy.kharon;

import io.grpc.ManagedChannel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ai.lzy.model.grpc.ChannelBuilder;
import ru.yandex.cloud.ml.platform.model.util.lock.LocalLockManager;
import ru.yandex.cloud.ml.platform.model.util.lock.LockManager;
import ai.lzy.priv.v2.LzyFsGrpc;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;

public class ServantConnectionManager {

    private static final Logger LOG = LogManager.getLogger(ServantConnectionManager.class);
    private final Map<URI, Connection> connectionMap = new HashMap<>();
    private final LockManager lockManager = new LocalLockManager();

    public LzyFsGrpc.LzyFsBlockingStub getOrCreate(URI uri) {
        LOG.info("getOrCreate connection for uri " + uri);
        final Lock lock = lockManager.getOrCreate(uri.toString());
        lock.lock();
        try {
            if (connectionMap.containsKey(uri)) {
                final Connection connection = connectionMap.get(uri);
                connection.increaseCounter();
                return connection.getStub();
            }

            final Connection connection = new Connection(uri);
            connectionMap.put(uri, connection);
            return connection.getStub();
        } finally {
            lock.unlock();
        }
    }

    public void shutdownConnection(URI uri) {
        final Lock lock = lockManager.getOrCreate(uri.toString());
        lock.lock();
        try {
            if (!connectionMap.containsKey(uri)) {
                LOG.warn("Attempt to shutdown non-existing connection to URI " + uri);
                return;
            }
            final Connection connection = connectionMap.get(uri);
            connection.decreaseCounter();
            if (connection.getNumReferences() == 0) {
                LOG.info("Number of references to URI equal to 1. Deleting connection to " + uri);
                connection.channel.shutdown();
                connectionMap.remove(uri);
            }
        } finally {
            lock.unlock();
        }
    }

    private static class Connection {

        private final LzyFsGrpc.LzyFsBlockingStub stub;
        private final ManagedChannel channel;
        private int numReferences = 1;

        Connection(URI uri) {
            LOG.info("Creating connection for uri " + uri);
            channel = ChannelBuilder
                .forAddress(uri.getHost(), uri.getPort())
                .usePlaintext()
                .build();
            stub = LzyFsGrpc.newBlockingStub(channel);
        }

        public LzyFsGrpc.LzyFsBlockingStub getStub() {
            return stub;
        }

        public void increaseCounter() {
            numReferences++;
        }

        public void decreaseCounter() {
            numReferences--;
        }

        public int getNumReferences() {
            return numReferences;
        }
    }
}
