package ai.lzy.servant.portal;

import ai.lzy.fs.LzyFsServer;
import ai.lzy.fs.fs.LzyInputSlot;
import ai.lzy.fs.fs.LzyOutputSlot;
import ai.lzy.fs.fs.LzySlot;
import ai.lzy.fs.snapshot.Snapshooter;
import ai.lzy.model.GrpcConverter;
import ai.lzy.model.JsonUtils;
import ai.lzy.priv.v2.LzyPortalApi.ConfigurePortalSlotsRequest;
import ai.lzy.priv.v2.LzyPortalApi.ConfigurePortalSlotsResponse;
import ai.lzy.priv.v2.LzyPortalApi.PortalSlotDesc;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static ai.lzy.model.UriScheme.Snapshot;

public class Portal {
    private static final Logger LOG = LogManager.getLogger(Portal.class);

    private final LzyFsServer fs;
    private final Snapshooter snapshooter;
    private final String portalTaskId;
    private final AtomicBoolean active = new AtomicBoolean(false);

    public Portal(String servantId, LzyFsServer fs) {
        this.fs = fs;
        this.snapshooter = Objects.requireNonNull(fs.getSlotConnectionManager().snapshooter());
        this.portalTaskId = "portal:" + UUID.randomUUID() + "@" + servantId;
    }

    public boolean start() {
        if (active.compareAndSet(false, true)) {

            return true;
        }
        return false;
    }

    public boolean stop() {
        return active.compareAndSet(true, false);
    }

    public boolean isActive() {
        return active.get();
    }

    public ConfigurePortalSlotsResponse configureSlots(ConfigurePortalSlotsRequest request) {
        LOG.info("Configure slots request.");

        if (!active.get()) {
            throw new IllegalStateException("Portal is not active.");
        }

        var response = ConfigurePortalSlotsResponse.newBuilder()
                .setSuccess(true);

        final Function<String, ConfigurePortalSlotsResponse> replyError = message -> {
            response.setSuccess(false);
            response.setDescription(message);
            return response.build();
        };

        for (PortalSlotDesc slotDesc : request.getSlotsList()) {
            LOG.info("Configure slot {}", portalSlotToSafeString(slotDesc));

            LzySlot lzySlot;
            try {
                var slot = GrpcConverter.from(slotDesc.getSlot());
                lzySlot = fs.createSlot(portalTaskId, slot, slotDesc.getChannelId(), /* fromPortal */ true);
            } catch (LzyFsServer.SlotCommandException e) {
                return replyError.apply(e.getMessage());
            }

            final URI slotUri = fs.getSlotsManager().resolveSlotUri(portalTaskId, lzySlot.name());
            final URI channelUri = URI.create(slotDesc.getChannelId());

            if (Snapshot.match(channelUri)) {
                if (lzySlot instanceof LzyOutputSlot) {
                    snapshooter.registerSlot(lzySlot, "snapshot://" + channelUri.getHost(), slotDesc.getChannelId());
                }

                if (lzySlot instanceof LzyInputSlot) {
                    // TODO:
                }
            }

            try {
                fs.connectSlot(portalTaskId, lzySlot, slotUri, /* fromPortal */ true);
            } catch (LzyFsServer.SlotCommandException e) {
                return replyError.apply(e.getMessage());
            }

        }

        return response.build();
    }

    private static String portalSlotToSafeString(PortalSlotDesc slotDesc) {
        var sb = new StringBuilder()
                .append("PortalSlotDesc{")
                .append("\"slot\": ").append(JsonUtils.printSingleLineRequest(slotDesc))
                .append(", \"storage\": ");

        switch (slotDesc.getStorageCase()) {
            case S3 -> sb.append("{\"kind\": \"s3\", \"url\": \"").append(slotDesc.getS3().getUrl()).append("\"}");
            default -> sb.append("\"").append(slotDesc.getStorageCase()).append("\"");
        }

        return sb.append("}").toString();
    }
}
