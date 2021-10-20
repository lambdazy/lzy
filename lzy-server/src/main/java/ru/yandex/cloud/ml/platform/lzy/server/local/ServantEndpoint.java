package ru.yandex.cloud.ml.platform.lzy.server.local;

import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.SlotStatus;
import ru.yandex.cloud.ml.platform.lzy.model.gRPCConverter;
import ru.yandex.cloud.ml.platform.lzy.server.channel.Endpoint;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServantGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.Servant;

import java.net.URI;
import java.util.UUID;

public class ServantEndpoint extends BaseEndpoint {
    private static final Logger LOG = LogManager.getLogger(ServantEndpoint.class);
    private final ManagedChannel servantChannel;

    public ServantEndpoint(Slot slot, URI uri, UUID taskId, ManagedChannel servantChannel) {
        super(slot, uri, taskId);
        this.servantChannel = servantChannel;
    }

    @Override
    public int connect(Endpoint endpoint) {
        try {
            final Servant.SlotCommandStatus rc = LzyServantGrpc.newBlockingStub(servantChannel)
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
        try {
            final Servant.SlotCommandStatus slotCommandStatus = LzyServantGrpc.newBlockingStub(servantChannel)
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
            final Servant.SlotCommandStatus rc = LzyServantGrpc.newBlockingStub(servantChannel)
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
        try {
            final Servant.SlotCommandStatus rc = LzyServantGrpc.newBlockingStub(servantChannel)
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
