package ru.yandex.cloud.ml.platform.lzy.server.local;

import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import jakarta.inject.Singleton;
import ru.yandex.cloud.ml.platform.lzy.model.GrpcConstant;
import ru.yandex.cloud.ml.platform.lzy.model.grpc.ChannelBuilder;
import ru.yandex.cloud.ml.platform.lzy.server.ConnectionManager;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServantGrpc;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Singleton
public class LocalConnectionManager implements ConnectionManager {
    private final Map<UUID, Connection> connections = new HashMap<>();

    private static class Connection {
        private final LzyServantGrpc.LzyServantBlockingStub stub;
        private final ManagedChannel channel;

        Connection(URI uri, UUID sessionId) {
            channel = ChannelBuilder
                    .forAddress(uri.getHost(), uri.getPort())
                    .usePlaintext()
                    .enableRetry(LzyServantGrpc.SERVICE_NAME)
                    .build();

            final Metadata metadata = new Metadata();
            metadata.put(GrpcConstant.SESSION_ID_METADATA_KEY, sessionId.toString());
            stub = MetadataUtils.attachHeaders(LzyServantGrpc.newBlockingStub(channel), metadata);
        }
    }

    @Override
    public synchronized LzyServantGrpc.LzyServantBlockingStub getOrCreate(URI uri, UUID sessionId) {
        if (connections.containsKey(sessionId)) {
            return connections.get(sessionId).stub;
        }

        final Connection connection = new Connection(uri, sessionId);
        connections.put(sessionId, connection);
        return connection.stub;
    }

    @Override
    public synchronized void shutdownConnection(UUID sessionId) {
        if (!connections.containsKey(sessionId)) {
            return;
        }

        final Connection connection = connections.remove(sessionId);
        connection.channel.shutdown();
    }
}
