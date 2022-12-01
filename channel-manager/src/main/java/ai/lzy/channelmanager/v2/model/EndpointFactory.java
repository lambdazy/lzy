package ai.lzy.channelmanager.v2.model;

import ai.lzy.channelmanager.v2.slot.SlotApiConnection;
import ai.lzy.channelmanager.v2.slot.SlotConnectionManager;
import ai.lzy.model.slot.SlotInstance;
import jakarta.inject.Singleton;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Singleton
public class EndpointFactory {

    private final Map<URI, Endpoint> endpointsCache = new ConcurrentHashMap<>();
    private final SlotConnectionManager slotConnectionManager;

    public EndpointFactory(SlotConnectionManager slotConnectionManager) {
        this.slotConnectionManager = slotConnectionManager;
    }

    public Endpoint createEndpoint(SlotInstance slot, Endpoint.SlotOwner owner, Endpoint.LifeStatus lifeStatus)
    {
        Endpoint endpoint = endpointsCache.get(slot.uri());
        SlotApiConnection slotApiConnection;
        if (endpoint != null && endpoint.getSlotApiConnection() != null) {
            slotApiConnection = endpoint.getSlotApiConnection();
        } else {
            slotApiConnection = slotConnectionManager.getOrCreateConnection(slot.uri());
        }
        endpoint = new Endpoint(slotApiConnection, slot, owner, lifeStatus, () -> removeEndpoint(slot.uri()));
        endpointsCache.put(slot.uri(), endpoint);
        return endpoint;
    }

    private void removeEndpoint(URI slotUri) {
        endpointsCache.remove(slotUri);
        slotConnectionManager.shutdownConnection(slotUri);
    }

}
