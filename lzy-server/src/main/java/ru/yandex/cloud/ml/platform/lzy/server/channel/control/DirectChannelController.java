package ru.yandex.cloud.ml.platform.lzy.server.channel.control;

import io.grpc.StatusRuntimeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.server.channel.ChannelController;
import ru.yandex.cloud.ml.platform.lzy.server.channel.ChannelEx;
import ru.yandex.cloud.ml.platform.lzy.server.channel.ChannelException;
import ru.yandex.cloud.ml.platform.lzy.server.local.Binding;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServantGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.Servant;

import java.text.MessageFormat;
import java.util.concurrent.ForkJoinPool;

public class DirectChannelController implements ChannelController {
    private static final Logger LOG = LogManager.getLogger(DirectChannelController.class);

    private final ChannelEx channel;
    private Binding input;

    public DirectChannelController(ChannelEx channel) {
        this.channel = channel;
    }

    @Override
    public ChannelController executeBind(Binding slot) throws ChannelException {
        switch (slot.slot().direction()) {
            case OUTPUT: {
                if (this.input != null && !this.input.equals(slot))
                    throw new ChannelException("Direct channel can not have two inputs");
                this.input = slot;
                if (channel.bound().filter(s -> s.slot().direction() == Slot.Direction.INPUT).mapToInt(s -> {
                    final int rc = connect(s, slot);
                    if (rc != 0)
                        LOG.warn(MessageFormat.format(
                            "Failure {2} while connecting {0} to {1}",
                            s.uri(), input.uri(), rc
                        ));
                    return rc;
                }).sum() > 0) {
                    throw new ChannelException("Unable to reconfigure channel");
                }
                break;
            }
            case INPUT: {
                if (input != null)
                    connect(slot, input);
                break;
            }
        }
        return this;
    }

    @Override
    public ChannelController executeUnBind(Binding slot) throws ChannelException {
        if (slot == input) {
            input = null;
            ForkJoinPool.commonPool().execute(() -> {
                channel.bound().filter(b -> !b.isInvalid()).forEach(this::disconnect);
            });
        }
        return this;
    }

    @Override
    public void executeDestroy() throws ChannelException {
        int rcSum = channel.bound().filter(s -> !s.equals(input)).mapToInt(this::close).sum();
        if (input != null)
            rcSum += close(input);
        if (rcSum > 0)
            throw new ChannelException("Failed to destroy channel");
    }

    private int connect(Binding from, Binding to) {
        try {
            final Servant.SlotCommandStatus rc = LzyServantGrpc.newBlockingStub(from.control())
                .configureSlot(
                    Servant.SlotCommand.newBuilder()
                        .setSlot(from.slot().name())
                        .setConnect(Servant.ConnectSlotCommand.newBuilder()
                            .setSlotUri(to.uri().toString())
                            .build()
                        )
                        .build()
                );
            return rc.hasRc() ? rc.getRc().getNumber() : 0;
        }
        catch (StatusRuntimeException sre) {
            LOG.warn("Unable to connect " + from + " to " + to + "\nCause:\n " + sre);
            return 0;
        }
    }

    private int disconnect(Binding from) {
        if (from.isInvalid()) { // skip invalid connections
            return 0;
        }
        try {
            final Servant.SlotCommandStatus rc = LzyServantGrpc.newBlockingStub(from.control())
                .configureSlot(
                    Servant.SlotCommand.newBuilder()
                        .setSlot(from.slot().name())
                        .setDisconnect(Servant.DisconnectCommand.newBuilder().build())
                        .build()
                );
            return rc.hasRc() ? rc.getRc().getNumber() : 0;
        }
        catch (StatusRuntimeException sre) {
            LOG.warn("Unable to disconnect " + from + "\n Cause:\n" + sre);
            return 0;
        }
    }

    private int close(Binding from) {
        try {
            final Servant.SlotCommandStatus rc = LzyServantGrpc.newBlockingStub(from.control())
                .configureSlot(
                    Servant.SlotCommand.newBuilder()
                        .setSlot(from.slot().name())
                        .setClose(Servant.CloseCommand.newBuilder().build())
                        .build()
                );
            return rc.hasRc() ? rc.getRc().getNumber() : 0;
        }
        catch (StatusRuntimeException sre) {
            LOG.warn("Unable to close binding: " + from.uri());
            return 0;
        }
    }
}
