package ru.yandex.cloud.ml.platform.lzy.server.local;

import io.grpc.StatusRuntimeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.GrpcConverter;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.SlotStatus;
import ru.yandex.cloud.ml.platform.lzy.server.channel.Endpoint;
import yandex.cloud.priv.datasphere.v2.lzy.LzyFsGrpc.LzyFsBlockingStub;
import yandex.cloud.priv.datasphere.v2.lzy.Servant;

import java.net.URI;
import java.nio.file.Path;
import java.util.Objects;
import java.util.UUID;

public class ServantEndpoint implements Endpoint {
    private static final Logger LOG = LogManager.getLogger(ServantEndpoint.class);

    private final Slot slot;
    private final URI slotUri;
    private final UUID sessionId;
    private final LzyFsBlockingStub fs;
    private final String tid;
    private boolean invalid = false;

    public ServantEndpoint(Slot slot, URI slotUri, UUID sessionId, LzyFsBlockingStub fs) {
        this.slot = slot;
        this.slotUri = slotUri;
        this.sessionId = sessionId;
        this.fs = fs;
        // [TODO] in case of terminal slot uri this will cause invalid tid assignment (terminal has no tids)
        this.tid = Path.of(slotUri.getPath()).getName(0).toString();
    }

    public URI uri() {
        return slotUri;
    }

    public Slot slot() {
        return slot;
    }

    @Override
    public UUID sessionId() {
        return sessionId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ServantEndpoint endpoint = (ServantEndpoint) o;
        return slotUri.equals(endpoint.slotUri);
    }

    @Override
    public int hashCode() {
        return Objects.hash(slotUri);
    }

    @Override
    public String toString() {
        return "(endpoint) {" + slotUri + "}";
    }

    @Override
    public boolean isInvalid() {
        return invalid;
    }

    @Override
    public void invalidate() {
        invalid = true;
    }

    @Override
    public int connect(URI endpoint) {
        if (isInvalid()) {
            LOG.warn("Attempt to connect to invalid endpoint " + this);
            return 1;
        }
        try {
            final Servant.SlotCommandStatus rc = fs.configureSlot(
                Servant.SlotCommand.newBuilder()
                    .setSlot(slot().name())
                    .setTid(tid)
                    .setConnect(Servant.ConnectSlotCommand.newBuilder()
                        .setSlotUri(endpoint.toString())
                        .build())
                    .build()
            );
            if (rc.hasRc() && rc.getRc().getCodeValue() != 0) {
                LOG.warn("Slot connection failed: " + rc.getRc().getDescription());
            }
            return rc.hasRc() ? rc.getRc().getCodeValue() : 0;
        } catch (StatusRuntimeException sre) {
            LOG.error("Unable to connect from: " + this + " to " + endpoint + "\nCause:\n " + sre);
            return 1;
        }
    }

    @Override
    public SlotStatus status() {
        if (isInvalid()) {
            LOG.warn("Attempt to get status of invalid endpoint " + this);
            return null;
        }
        try {
            final Servant.SlotCommandStatus slotCommandStatus = fs
                .configureSlot(
                    Servant.SlotCommand.newBuilder()
                        .setTid(tid)
                        .setSlot(slot().name())
                        .setStatus(Servant.StatusCommand.newBuilder().build())
                        .build()
                );
            return GrpcConverter.from(slotCommandStatus.getStatus());
        } catch (StatusRuntimeException e) {
            LOG.warn("Exception during slotStatus " + e);
            return null;
        }
    }

    @Override
    public void snapshot(String snapshotId, String entryId) {
        if (isInvalid()) { // skip invalid connections
            return;
        }
        try {
            final Servant.SlotCommandStatus rc = fs
                .configureSlot(
                    Servant.SlotCommand.newBuilder()
                        .setTid(tid)
                        .setSlot(slot().name())
                        .setSnapshot(Servant.SnapshotCommand.newBuilder()
                            .setSnapshotId(snapshotId)
                            .setEntryId(entryId)
                            .build())
                        .build());
            System.err.println("--> " + rc.toString());
        } catch (StatusRuntimeException sre) {
            LOG.error("Unable to send snapshot command " + this + "\n Cause:\n" + sre);
        }
    }

    @Override
    public int disconnect() {
        if (isInvalid()) { // skip invalid connections
            return 0;
        }
        try {
            final Servant.SlotCommandStatus rc = fs
                .configureSlot(
                    Servant.SlotCommand.newBuilder()
                        .setTid(tid)
                        .setSlot(slot().name())
                        .setDisconnect(Servant.DisconnectCommand.newBuilder().build())
                        .build()
                );
            return rc.hasRc() ? rc.getRc().getCodeValue() : 0;
        } catch (StatusRuntimeException sre) {
            LOG.warn("Unable to disconnect " + this + "\n Cause:\n" + sre);
            return 0;
        }
    }

    @Override
    public int destroy() {
        LOG.info("Destroying slot " + slot().name() + " for servant " + uri());
        if (isInvalid()) { // skip invalid connections
            return 0;
        }
        try {
            final Servant.SlotCommandStatus rc = fs
                .configureSlot(
                    Servant.SlotCommand.newBuilder()
                        .setTid(tid)
                        .setSlot(slot().name())
                        .setDestroy(Servant.DestroyCommand.newBuilder().build())
                        .build()
                );
            invalidate();
            return rc.hasRc() ? rc.getRc().getCodeValue() : 0;
        } catch (StatusRuntimeException sre) {
            LOG.warn("Unable to close " + this);
            return 0;
        }
    }
}
