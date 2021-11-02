package ru.yandex.cloud.ml.platform.lzy.server.local;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import jakarta.inject.Singleton;
import ru.yandex.cloud.ml.platform.lzy.server.ConnectionManager;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServantGrpc;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

@Singleton
public class LocalConnectionManager implements ConnectionManager {
    private final Map<URI, Connection> connections = new HashMap<>();

    private static class Connection {
        private final LzyServantGrpc.LzyServantBlockingStub stub;
        private final ManagedChannel channel;

        Connection(URI uri) {
            channel = ManagedChannelBuilder
                .forAddress(uri.getHost(), uri.getPort())
                .usePlaintext()
                .build();
            stub = LzyServantGrpc.newBlockingStub(channel);
        }
    }

    @Override
    public synchronized LzyServantGrpc.LzyServantBlockingStub getOrCreate(URI uri) {
        if (connections.containsKey(uri)) {
            return connections.get(uri).stub;
        }

        final Connection connection = new Connection(uri);
        connections.put(uri, connection);
        return connection.stub;
    }

    @Override
    public synchronized void shutdownConnection(URI uri) {
        if (!connections.containsKey(uri)) {
            return;
        }

        final Connection connection = connections.remove(uri);
        connection.channel.shutdown();
    }
}
