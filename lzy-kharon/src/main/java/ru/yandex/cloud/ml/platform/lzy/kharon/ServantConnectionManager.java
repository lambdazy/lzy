package ru.yandex.cloud.ml.platform.lzy.kharon;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServantGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServantGrpc.LzyServantBlockingStub;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class ServantConnectionManager {
    private static final Logger LOG = LogManager.getLogger(ServantConnectionManager.class);
    private final Map<URI, Connection> connectionMap = new HashMap<>();

    private static class Connection {
        private final LzyServantBlockingStub stub;
        private final ManagedChannel channel;
        private int numReferences = 1;

        Connection(URI uri) {
            LOG.info("Creating connection for uri " + uri);
            channel = ManagedChannelBuilder
                .forAddress(uri.getHost(), uri.getPort())
                .usePlaintext()
                .build();
            stub = LzyServantGrpc.newBlockingStub(channel);
        }

        public LzyServantBlockingStub getStub() {
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

    public synchronized LzyServantBlockingStub getOrCreate(URI uri) {
        LOG.info("getOrCreate connection for uri " + uri);
        if (connectionMap.containsKey(uri)) {
            final Connection connection = connectionMap.get(uri);
            connection.increaseCounter();
            return connection.getStub();
        }

        final Connection connection = new Connection(uri);
        connectionMap.put(uri, connection);
        return connection.getStub();
    }

    public synchronized void shutdownConnection(URI uri) {
        if (!connectionMap.containsKey(uri)) {
            LOG.warn("Attempt to shutdown non-existing connection to URI " + uri);
        }
        final Connection connection = connectionMap.get(uri);
        connection.decreaseCounter();
        if (connection.getNumReferences() == 0) {
            LOG.info("Number of references to URI equal to 1. Deleting it.");
            connection.channel.shutdown();
            connectionMap.remove(uri);
        }
    }
}
