package ai.lzy.channelmanager.v2.grpc;

import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.v1.slots.LzySlotsApiGrpc;
import com.google.common.net.HostAndPort;
import io.grpc.ManagedChannel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class SlotConnectionManager {

    private static final Logger LOG = LogManager.getLogger(SlotConnectionManager.class);
    private final Map<HostAndPort, SlotConnectionManager.Connection> connectionMap = new HashMap<>();

    public synchronized LzySlotsApiGrpc.LzySlotsApiBlockingStub getOrCreate(URI uri) {
        LOG.debug("[getOrCreate] slot-api connection for uri={}", uri);
        final HostAndPort uriHostPort = HostAndPort.fromParts(uri.getHost(), uri.getPort());
        if (connectionMap.containsKey(uriHostPort)) {
            final Connection connection = connectionMap.get(uriHostPort);
            connection.increaseCounter();
            return connection.getStub();
        }

        final Connection connection = new Connection(uri);
        connectionMap.put(uriHostPort, connection);
        LOG.debug("[getOrCreate] slot-api connection for uri={} done", uri);
        return connection.getStub();
    }

    public synchronized void shutdownConnection(URI uri) {
        LOG.debug("[shutdownConnection] slot-api connection for uri={}", uri);
        final HostAndPort uriHostPort = HostAndPort.fromParts(uri.getHost(), uri.getPort());
        if (!connectionMap.containsKey(uriHostPort)) {
            LOG.warn("[shutdownConnection] slot-api connection for uri={} skipped, connection doesn't exist", uri);
            return;
        }
        final Connection connection = connectionMap.get(uriHostPort);
        connection.decreaseCounter();
        if (connection.getNumReferences() == 0) {
            LOG.info("[shutdownConnection] Deleting slot-api connection, uri={}", uri);
            connection.channel.shutdown();
            connectionMap.remove(uriHostPort);
        }
        LOG.debug("[shutdownConnection] slot-api connection for uri={} done", uri);
    }

    private static class Connection {

        private final LzySlotsApiGrpc.LzySlotsApiBlockingStub stub;
        private final ManagedChannel channel;
        private int numReferences = 1;

        Connection(URI uri) {
            LOG.debug("Creating slot-api connection for uri={}", uri);
            channel = ChannelBuilder
                .forAddress(uri.getHost(), uri.getPort())
                .usePlaintext()
                .build();
            stub = LzySlotsApiGrpc.newBlockingStub(channel);
            LOG.info("Creating slot-api connection for uri={} done", uri);
        }

        public LzySlotsApiGrpc.LzySlotsApiBlockingStub getStub() {
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
