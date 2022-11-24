package ai.lzy.channelmanager.v2.slot;

import ai.lzy.channelmanager.ChannelManagerConfig;
import com.google.common.net.HostAndPort;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Singleton
public class SlotConnectionManager {

    private static final Logger LOG = LogManager.getLogger(SlotConnectionManager.class);

    private final Map<HostAndPort, SlotApiConnection> connectionMap;
    private final ChannelManagerConfig config;

    public SlotConnectionManager(ChannelManagerConfig config) {
        this.connectionMap = new ConcurrentHashMap<>();
        this.config = config;
    }

    public synchronized SlotApiConnection getOrCreateConnection(URI uri) {
        LOG.debug("[getOrCreateConnection], uri={}", uri);
        final HostAndPort address = HostAndPort.fromParts(uri.getHost(), uri.getPort());
        int refCount;
        if (connectionMap.containsKey(address)) {
            final SlotApiConnection connection = connectionMap.get(address);
            refCount = connection.increaseCounter();
            LOG.debug("[getOrCreateConnection] found, {} references, uri={},", refCount, uri);
            return connection;
        }

        final SlotApiConnection connection = new SlotApiConnection(config.getIam().createRenewableToken(), address);
        connectionMap.put(address, connection);
        refCount = 1;
        LOG.debug("[getOrCreateConnection] created, {} references, uri={}", refCount, uri);
        return connection;
    }

    public synchronized void shutdownConnection(URI uri) {
        LOG.debug("[shutdownConnection], uri={}", uri);
        final HostAndPort address = HostAndPort.fromParts(uri.getHost(), uri.getPort());
        if (!connectionMap.containsKey(address)) {
            LOG.warn("[shutdownConnection] skipped, connection doesn't exist, uri={}", uri);
            return;
        }
        final SlotApiConnection connection = connectionMap.get(address);
        int refCount = connection.decreaseCounter();
        if (refCount == 0) {
            LOG.debug("[shutdownConnection] connection removed, {} references left, uri={}", refCount, uri);
            connection.shutdown();
            connectionMap.remove(address);
        }
        LOG.debug("[shutdownConnection] done, {} references left, uri={}", refCount, uri);
    }

}
