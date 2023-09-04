package ai.lzy.channelmanager.grpc;

import ai.lzy.channelmanager.config.ChannelManagerConfig;
import ai.lzy.util.auth.credentials.RenewableJwt;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalNotification;
import com.google.common.net.HostAndPort;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;

@Singleton
public class SlotConnectionManager {

    private static final Logger LOG = LogManager.getLogger(SlotConnectionManager.class);

    private final LoadingCache<HostAndPort, SlotGrpcConnection> connections;
    private final RenewableJwt iamToken;

    public SlotConnectionManager(ChannelManagerConfig config,
                                 @Named("ChannelManagerIamToken") RenewableJwt iamToken)
    {
        this.connections = CacheBuilder.newBuilder()
            .expireAfterAccess(config.getConnections().getCacheTTL())
            .concurrencyLevel(config.getConnections().getCacheConcurrencyLevel())
            .removalListener(this::onRemove)
            .build(CacheLoader.from(this::onLoad));
        this.iamToken = iamToken;
    }

    @PreDestroy
    public void shutdown() {
        connections.cleanUp();
    }

    public SlotGrpcConnection getConnection(URI uri) {
        final HostAndPort address = HostAndPort.fromParts(uri.getHost(), uri.getPort());
        return connections.getUnchecked(address);
    }

    private SlotGrpcConnection onLoad(HostAndPort address) {
        final var connection = new SlotGrpcConnection(iamToken, address);
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
