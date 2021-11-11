package ru.yandex.cloud.ml.platform.lzy.servant.slots;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.model.util.lock.LocalLockManager;
import ru.yandex.cloud.ml.platform.model.util.lock.LockManager;
import yandex.cloud.priv.datasphere.v2.lzy.Servant;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;

public class SlotConnectionManager {
    private static final Logger LOG = LogManager.getLogger(SlotConnectionManager.class);
    private final Map<URI, Connection> connections = new ConcurrentHashMap<>();
    private final Map<String, List<URI>> connectedChannels = new ConcurrentHashMap<>();
    private final LockManager lockManager = new LocalLockManager();

    private static class Connection {
        private final SlotController controller;
        private final ManagedChannel channel;

        Connection(URI uri, ControllerCreator controllerCreator) {
            channel = ManagedChannelBuilder
                .forAddress(uri.getHost(), uri.getPort())
                .usePlaintext()
                .build();
            controller = controllerCreator.create(channel);
        }
    }

    public interface SlotController {
        Iterator<Servant.Message> openOutputSlot(Servant.SlotRequest request);
    }

    public interface ControllerCreator {
        SlotController create(ManagedChannel channel);
    }

    public synchronized SlotController getOrCreate(String slotName, URI slotUri, ControllerCreator controllerCreator) {
        LOG.info("Create connection to " + slotUri);
        final URI uri = getOnlyHostPortURI(slotUri);

        final Lock lock = lockManager.getOrCreate(slotName);
        lock.lock();
        try {
            if (connections.containsKey(slotUri)) {
                return connections.get(slotUri).controller;
            }

            final Connection connection = new Connection(uri, controllerCreator);
            connections.put(slotUri, connection);
            if (!connectedChannels.containsKey(slotName)) {
                connectedChannels.put(slotName, Collections.singletonList(slotUri));
            } else {
                final List<URI> uris = connectedChannels.get(slotName);
                uris.add(slotUri);
            }
            return connection.controller;
        } finally {
            lock.unlock();
        }
    }

    public synchronized void shutdownConnections(String slotName) {
        LOG.info("Shutdown connections from slot " + slotName);

        final Lock lock = lockManager.getOrCreate(slotName);
        lock.lock();
        try {
            if (!connectedChannels.containsKey(slotName)) {
                LOG.info("No connections from slot " + slotName + " exit");
                return;
            }

            for (URI uri : connectedChannels.get(slotName)) {
                final Connection connection = connections.remove(uri);
                connection.channel.shutdown();
            }
        } finally {
            lock.unlock();
        }
    }

    private URI getOnlyHostPortURI(URI fullUri) {
        try {
            return new URI(fullUri.getScheme(), null, fullUri.getHost(), fullUri.getPort(), null, null, null);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
