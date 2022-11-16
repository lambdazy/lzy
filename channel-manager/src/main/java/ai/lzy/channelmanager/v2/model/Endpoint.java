package ai.lzy.channelmanager.v2.model;

import ai.lzy.channelmanager.v2.grpc.SlotConnectionManager;
import ai.lzy.model.grpc.ProtoConverter;
import ai.lzy.model.slot.Slot;
import ai.lzy.model.slot.SlotInstance;
import ai.lzy.v1.channel.v2.LCMS;
import ai.lzy.v1.common.LMS;
import ai.lzy.v1.slots.LzySlotsApiGrpc;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Endpoint {

    private static final Logger LOG = LogManager.getLogger(Endpoint.class);
    private static final Map<URI, Endpoint> ENDPOINTS_CACHE = new ConcurrentHashMap<>();
    private static final SlotConnectionManager SLOT_CONNECTION_MANAGER = new SlotConnectionManager();

    private final LzySlotsApiGrpc.LzySlotsApiBlockingStub slotApiStub;
    private final SlotInstance slot;
    private final SlotOwner slotOwner;
    private final LifeStatus lifeStatus;

    private Endpoint(SlotInstance slot, SlotOwner slotOwner, LifeStatus lifeStatus) {
        this.slotApiStub = SLOT_CONNECTION_MANAGER.getOrCreate(slot.uri());
        this.slotOwner = slotOwner;
        this.slot = slot;
        this.lifeStatus = lifeStatus;
    }

    public static synchronized Endpoint fromSlot(SlotInstance slot, SlotOwner owner, LifeStatus lifeStatus) {
        return ENDPOINTS_CACHE.computeIfAbsent(slot.uri(), uri -> new Endpoint(slot, owner, lifeStatus));
    }

    public static Endpoint fromProto(LMS.SlotInstance instance, LCMS.BindRequest.SlotOwner origin) {
        return fromSlot(ProtoConverter.fromProto(instance), SlotOwner.fromProto(origin), LifeStatus.BINDING);
    }

    public LzySlotsApiGrpc.LzySlotsApiBlockingStub getSlotApiStub() {
        return slotApiStub;
    }

    public URI uri() {
        return slot.uri();
    }

    public String channelId() {
        return slot.channelId();
    }

    public SlotInstance slot() {
        return slot;
    }

    public SlotOwner slotOwner() {
        return slotOwner;
    }

    public Slot.Direction slotDirection() {
        return slot.spec().direction();
    }

    public LifeStatus status() {
        return lifeStatus;
    }

    public enum SlotOwner {
        WORKER,
        PORTAL,
        ;

        public static SlotOwner fromProto(LCMS.BindRequest.SlotOwner origin) {
            return SlotOwner.valueOf(origin.name());
        }
    }

    public enum LifeStatus {
        BINDING,
        BOUND,
        UNBINDING,
    }
}
