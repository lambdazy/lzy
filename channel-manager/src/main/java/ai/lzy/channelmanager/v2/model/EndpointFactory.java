package ai.lzy.channelmanager.v2.model;

import ai.lzy.channelmanager.v2.slot.SlotApiConnection;
import ai.lzy.channelmanager.v2.slot.SlotConnectionManager;
import ai.lzy.model.grpc.ProtoConverter;
import ai.lzy.model.slot.SlotInstance;
import ai.lzy.v1.channel.v2.LCMS;
import ai.lzy.v1.common.LMS;
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

    public Endpoint createEndpoint(LMS.SlotInstance slot, LCMS.BindRequest.SlotOwner owner,
                                   Endpoint.LifeStatus lifeStatus)
    {
        return createEndpoint(ProtoConverter.fromProto(slot), fromProto(owner), lifeStatus);
    }

    public Endpoint createEndpoint(SlotInstance slot, Endpoint.SlotOwner owner,
                                   Endpoint.LifeStatus lifeStatus)
    {
        Endpoint endpoint = endpointsCache.get(slot.uri());
        SlotApiConnection slotApiConnection = endpoint.getSlotApiConnection();
        if (endpoint.getSlotApiConnection() != null) {
            slotApiConnection = slotConnectionManager.getOrCreateConnection(slot.uri());
        }
        return endpointsCache.put(slot.uri(),
            new Endpoint(slotApiConnection, slot, owner, lifeStatus, () -> removeEndpoint(slot.uri())));
    }

    private void removeEndpoint(URI slotUri) {
        endpointsCache.remove(slotUri);
        slotConnectionManager.shutdownConnection(slotUri);
    }

    private Endpoint.SlotOwner fromProto(LCMS.BindRequest.SlotOwner origin) {
        return Endpoint.SlotOwner.valueOf(origin.name());
    }

}
