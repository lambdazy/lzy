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
import java.util.HashSet;
import java.util.Set;

public class DirectChannelController implements ChannelController {
    private static final Logger LOG = LogManager.getLogger(DirectChannelController.class);

    private final ChannelEx channel;
    private Binding input;
    private final Set<Binding> outputs = new HashSet<>();

    public DirectChannelController(ChannelEx channel) {
        this.channel = channel;
    }

    @Override
    public ChannelController executeBind(Binding slot) throws ChannelException {
        switch (slot.slot().direction()) {
            case OUTPUT: {
                if (this.input != null && !this.input.equals(slot)) {
                    throw new ChannelException("Direct channel can not have two inputs");
                }
                this.input = slot;
                if (channel.bound().filter(s -> s.slot().direction() == Slot.Direction.INPUT).mapToInt(s -> {
                    final int rc = connect(s, slot);
                    if (rc != 0) {
                        LOG.warn(MessageFormat.format(
                            "Failure {2} while connecting {0} to {1}",
                            s.uri(), input.uri(), rc
                        ));
                    }
                    return rc;
                }).sum() > 0) {
                    throw new ChannelException("Unable to reconfigure channel");
                }
                break;
            }
            case INPUT: {
                outputs.add(slot);
                if (input != null) {
                    connect(slot, input);
                }
                break;
            }
        }
        return this;
    }

    @Override
    public ChannelController executeUnBind(Binding binding) throws ChannelException {
        if (binding.slot().direction() == Slot.Direction.INPUT) {
            if (outputs.remove(binding)) {
                int rcSum = disconnect(binding);
                rcSum += destroy(binding);
                if (outputs.isEmpty()) {
                    rcSum += disconnect(input);
                }
                if (rcSum > 0) {
                    throw new ChannelException("Failed to unbind " + binding.uri());
                }
            } else {
                LOG.warn("Trying to unbind non-bound endpoint: {}", binding.uri());
            }
        } else if (outputs.isEmpty()) {
            final int rc = destroy(input);
            if (rc > 0) {
                throw new ChannelException("Failed to unbind " + input.uri());
            }
            input = null;
        }
        return this;
    }

    @Override
    public void executeDestroy() throws ChannelException {
        int rcSum = outputs.stream().mapToInt(this::destroy).sum();
        if (input != null) {
            rcSum += destroy(input);
        }
        if (rcSum > 0) {
            throw new ChannelException("Failed to destroy channel");
        }
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
        } catch (StatusRuntimeException sre) {
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
        } catch (StatusRuntimeException sre) {
            LOG.warn("Unable to disconnect " + from + "\n Cause:\n" + sre);
            return 0;
        }
    }

    private int destroy(Binding from) {
        try {
            final Servant.SlotCommandStatus rc = LzyServantGrpc.newBlockingStub(from.control())
                .configureSlot(
                    Servant.SlotCommand.newBuilder()
                        .setSlot(from.slot().name())
                        .setDestroy(Servant.DestroyCommand.newBuilder().build())
                        .build()
                );
            from.invalidate();
            return rc.hasRc() ? rc.getRc().getNumber() : 0;
        } catch (StatusRuntimeException sre) {
            LOG.warn("Unable to close binding: " + from.uri());
            return 0;
        }
    }
}
