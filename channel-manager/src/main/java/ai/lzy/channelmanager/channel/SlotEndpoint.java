package ai.lzy.channelmanager.channel;

import static ai.lzy.model.GrpcConverter.to;

import ai.lzy.model.GrpcConverter;
import ai.lzy.model.Slot;
import ai.lzy.model.SlotConnectionManager;
import ai.lzy.model.SlotInstance;
import ai.lzy.model.SlotStatus;
import ai.lzy.v1.LzyFsApi;
import ai.lzy.v1.LzyFsGrpc;
import io.grpc.StatusRuntimeException;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SlotEndpoint implements Endpoint {
    private static final Logger LOG = LogManager.getLogger(SlotEndpoint.class);
    private static final Map<SlotInstance, SlotEndpoint> ENDPOINTS_CACHE = new ConcurrentHashMap<>();
    private static final SlotConnectionManager SLOT_CONNECTION_MANAGER = new SlotConnectionManager();

    private final SlotInstance slot;
    private final LzyFsGrpc.LzyFsBlockingStub fs;
    private boolean invalid = false;

    private SlotEndpoint(SlotInstance slot) {
        LOG.info("Creating new SlotEndpoint for slot {} with uri {}", slot.name(), slot.uri());
        this.slot = slot;
        this.fs = SLOT_CONNECTION_MANAGER.getOrCreate(slot.uri());
    }

    public static synchronized SlotEndpoint getInstance(SlotInstance slotInstance) {
        final SlotEndpoint slotEndpoint = ENDPOINTS_CACHE.get(slotInstance);
        if (slotEndpoint == null) {
            final SlotEndpoint endpoint = new SlotEndpoint(slotInstance);
            ENDPOINTS_CACHE.put(slotInstance, endpoint);
            return endpoint;
        }
        return slotEndpoint;
    }

    @Override
    public URI uri() {
        return slot.uri();
    }

    @Override
    public Slot slotSpec() {
        return slot.spec();
    }

    @Override
    public SlotInstance slotInstance() {
        return slot;
    }

    @Override
    public String taskId() {
        return slot.taskId();
    }

    @Override
    public String toString() {
        return "(endpoint) {" + uri() + "}";
    }

    @Override
    public boolean isInvalid() {
        return invalid;
    }

    @Override
    public void invalidate() {
        invalid = true;
//        SLOT_CONNECTION_MANAGER.shutdownConnection(slot.uri());
        synchronized (SlotEndpoint.class) {
            ENDPOINTS_CACHE.remove(slotInstance());
        }
    }

    @Override
    public int connect(SlotInstance slotInstance) {
        if (isInvalid()) {
            LOG.warn("Attempt to connect to invalid endpoint " + this);
            return 1;
        }
        try {
            final LzyFsApi.SlotCommandStatus rc = fs.connectSlot(
                LzyFsApi.ConnectSlotRequest.newBuilder()
                    .setFrom(to(slotInstance()))
                    .setTo(to(slotInstance))
                .build()
            );
            if (rc.hasRc() && rc.getRc().getCodeValue() != 0) {
                LOG.warn("Slot connection failed: " + rc.getRc().getDescription());
            }
            return rc.hasRc() ? rc.getRc().getCodeValue() : 0;
        } catch (StatusRuntimeException sre) {
            LOG.error("Unable to connect from: {} to {}\nCause:\n ", this, slotInstance, sre);
            return 1;
        }
    }

    @Override
    @Nullable
    public SlotStatus status() {
        if (isInvalid()) {
            LOG.warn("Attempt to get status of invalid endpoint " + this);
            return null;
        }
        try {
            final LzyFsApi.SlotCommandStatus slotCommandStatus = fs.statusSlot(
                LzyFsApi.StatusSlotRequest.newBuilder()
                    .setSlotInstance(to(slotInstance()))
                    .build());
            return GrpcConverter.from(slotCommandStatus.getStatus());
        } catch (StatusRuntimeException e) {
            LOG.warn("Exception during slotStatus " + e);
            return null;
        }
    }

    @Override
    public int disconnect() {
        if (isInvalid()) { // skip invalid connections
            return 0;
        }
        try {
            final LzyFsApi.SlotCommandStatus rc = fs.disconnectSlot(
                LzyFsApi.DisconnectSlotRequest.newBuilder()
                    .setSlotInstance(to(slotInstance()))
                    .build());
            return rc.hasRc() ? rc.getRc().getCodeValue() : 0;
        } catch (StatusRuntimeException sre) {
            LOG.warn("Unable to disconnect " + this + "\n Cause:\n" + sre);
            return 0;
        }
    }

    @Override
    public int destroy() {
        LOG.info("Destroying slot " + slotSpec().name() + " for servant " + uri());
        if (isInvalid()) { // skip invalid connections
            return 0;
        }
        try {
            final LzyFsApi.SlotCommandStatus rc = fs.destroySlot(
                LzyFsApi.DestroySlotRequest.newBuilder()
                    .setSlotInstance(to(slotInstance()))
                    .build());
            return rc.hasRc() ? rc.getRc().getCodeValue() : 0;
        } catch (StatusRuntimeException sre) {
            LOG.warn("Unable to close " + this, sre);
            return 0;
        } finally {
            invalidate();
        }
    }
}
