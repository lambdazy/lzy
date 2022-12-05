package ai.lzy.channelmanager.v2.grpc;

import ai.lzy.channelmanager.v2.config.ChannelManagerConfig;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalNotification;
import com.google.common.net.HostAndPort;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.util.concurrent.TimeUnit;

@Singleton
public class SlotConnectionManager {

    private static final Logger LOG = LogManager.getLogger(SlotConnectionManager.class);

    private final LoadingCache<HostAndPort, SlotGrpcConnection> connections;
    private final ChannelManagerConfig config;

    public SlotConnectionManager(ChannelManagerConfig config) {
        this.config = config;
        this.connections = CacheBuilder.newBuilder()
            .expireAfterAccess(config.getConnections().getCacheTtlSeconds(), TimeUnit.SECONDS)
            .concurrencyLevel(config.getConnections().getCacheConcurrencyLevel())
            .removalListener(this::onRemove)
            .build(CacheLoader.from(this::onLoad));
    }

    public SlotGrpcConnection getConnection(URI uri) {
        final HostAndPort address = HostAndPort.fromParts(uri.getHost(), uri.getPort());
        return connections.getUnchecked(address);
    }

    private SlotGrpcConnection onLoad(HostAndPort address) {
        final var connection = new SlotGrpcConnection(config.getIam().createRenewableToken(), address);
        LOG.debug("Connection created, address={}", address);
        return connection;
    }

    private void onRemove(RemovalNotification<HostAndPort, SlotGrpcConnection> removalNotification) {
        HostAndPort address = removalNotification.getKey();
        SlotGrpcConnection connection = removalNotification.getValue();
        if (connection == null) {
            LOG.warn("Connection with address {} not found, cannot shutdown", removalNotification.getKey());
            return;
        }
        LOG.debug("Connection with address {} remove and shutdown: {}", address, removalNotification.getCause());
        connection.shutdown();
    }

}
