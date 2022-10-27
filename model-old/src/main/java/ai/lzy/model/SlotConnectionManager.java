package ai.lzy.model;

import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.v1.deprecated.LzyFsGrpc;
import io.grpc.ManagedChannel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class SlotConnectionManager {

    private static final Logger LOG = LogManager.getLogger(SlotConnectionManager.class);
    private final Map<URI, Connection> connectionMap = new HashMap<>();

    public synchronized LzyFsGrpc.LzyFsBlockingStub getOrCreate(URI uri) {
        LOG.info("getOrCreate connection for uri " + uri);
        if (connectionMap.containsKey(uri) /* TODO: fix potential bug: not by uri, by host-port*/) {
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
            return;
        }
        final Connection connection = connectionMap.get(uri);
        connection.decreaseCounter();
        if (connection.getNumReferences() == 0) {
            LOG.info("Number of references to URI equal to 1. Deleting connection to " + uri);
            connection.channel.shutdown();
            connectionMap.remove(uri);
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
