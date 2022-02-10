package ru.yandex.cloud.ml.platform.lzy.server.local;

import io.grpc.StatusRuntimeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.SlotStatus;
import ru.yandex.cloud.ml.platform.lzy.model.gRPCConverter;
import ru.yandex.cloud.ml.platform.lzy.server.channel.Endpoint;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServantGrpc.LzyServantBlockingStub;
import yandex.cloud.priv.datasphere.v2.lzy.Servant;

import java.net.URI;
import java.util.Objects;
import java.util.UUID;

public class ServantEndpoint implements Endpoint {
    private static final Logger LOG = LogManager.getLogger(ServantEndpoint.class);

    private final URI uri;
    private final Slot slot;
    private final UUID sessionId;
    private boolean invalid = false;
    private final LzyServantBlockingStub servant;

    public ServantEndpoint(Slot slot, URI uri, UUID sessionId, LzyServantBlockingStub servant) {
        this.uri = uri;
        this.slot = slot;
        this.sessionId = sessionId;
        this.servant = servant;
    }

    public URI uri() {
        return uri;
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
        return uri.equals(endpoint.uri);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uri);
    }

    @Override
    public String toString() {
        return "(endpoint) {" + uri + "}";
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
    public int connect(Endpoint endpoint) {
        if (isInvalid()) {
            LOG.warn("Attempt to connect to invalid endpoint " + this);
            return 1;
        }
        try {
            final Servant.SlotCommandStatus rc = servant
                .configureSlot(
                    Servant.SlotCommand.newBuilder()
                        .setSlot(slot().name())
                        .setConnect(Servant.ConnectSlotCommand.newBuilder()
                            .setSlotUri(endpoint.uri().toString())
                            .build()
                        )
                        .build()
                );
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
            final Servant.SlotCommandStatus slotCommandStatus = servant
                .configureSlot(
                    Servant.SlotCommand.newBuilder()
                        .setSlot(slot().name())
                        .setStatus(Servant.StatusCommand.newBuilder().build())
                        .build()
                );
            return gRPCConverter.from(slotCommandStatus.getStatus());
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
            final Servant.SlotCommandStatus rc = servant
                .configureSlot(
                    Servant.SlotCommand.newBuilder()
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
        if (isInvalid()) { // skip invalid connections
            return 0;
        }
        try {
            final Servant.SlotCommandStatus rc = servant
                .configureSlot(
                    Servant.SlotCommand.newBuilder()
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
