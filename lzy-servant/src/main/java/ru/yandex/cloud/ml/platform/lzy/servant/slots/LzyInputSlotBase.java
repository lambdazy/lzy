package ru.yandex.cloud.ml.platform.lzy.servant.slots;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.gRPCConverter;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzyInputSlot;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServantGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.Operations;
import yandex.cloud.priv.datasphere.v2.lzy.Servant;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

public abstract class LzyInputSlotBase extends LzySlotBase implements LzyInputSlot {
    private static final Logger LOG = LogManager.getLogger(LzyInputSlotBase.class);

    private final String tid;
    private long offset = 0;
    private URI connected;
    private ManagedChannel servantSlotCh;
    private LzyServantGrpc.LzyServantBlockingStub connectedSlotController;

    LzyInputSlotBase(String tid, Slot definition) {
        super(definition);
        this.tid = tid;
    }

    @Override
    public void connect(URI slotUri) {
        LOG.info("LzyInputSlotBase:: Attempt to connect to " + slotUri);
        connected = slotUri;
        servantSlotCh = ManagedChannelBuilder.forAddress(slotUri.getHost(), slotUri.getPort())
            .usePlaintext()
            .build();
        connectedSlotController = LzyServantGrpc.newBlockingStub(servantSlotCh);
        state(Operations.SlotStatus.State.OPEN);
    }

    @Override
    public void disconnect() {
        servantSlotCh.shutdown();
        connected = null;
        servantSlotCh = null;
        state(Operations.SlotStatus.State.SUSPENDED);
    }

    protected void readAll() {
        final Iterator<Servant.Message> msgIter = connectedSlotController.openOutputSlot(Servant.SlotRequest.newBuilder()
            .setSlot(connected.getPath())
            .setOffset(offset)
            .build());
        state(Operations.SlotStatus.State.OPEN);
        try {
            while (msgIter.hasNext()) {
                final Servant.Message next = msgIter.next();
                if (next.hasChunk()) {
                    final ByteString chunk = next.getChunk();
                    try {
                        LOG.info("From {} chunk received {}", name(), chunk.toString(StandardCharsets.UTF_8));
                        onChunk(chunk);
                    } catch (IOException ioe) {
                        LOG.warn(
                            "Unable write chunk of data of size " + chunk.size() + " to input slot " + name(),
                            ioe
                        );
                    } finally {
                        offset += chunk.size();
                    }
                } else if (next.getControl() == Servant.Message.Controls.EOS) {
                    break;
                }
            }
        }
        finally {
            LOG.info("Closing slot {}",connected.getPath());
            //noinspection ResultOfMethodCallIgnored
            connectedSlotController.configureSlot(Servant.SlotCommand
                .newBuilder()
                .setClose(Servant.CloseCommand.newBuilder().build())
                .setSlot(connected.getPath())
                .build());
            state(Operations.SlotStatus.State.SUSPENDED);
        }
    }

    @Override
    public void close() {
        state(Operations.SlotStatus.State.CLOSED);
    }

    @Override
    public Operations.SlotStatus status() {
        final Operations.SlotStatus.Builder builder = Operations.SlotStatus.newBuilder()
            .setState(state())
            .setPointer(offset)
            .setDeclaration(gRPCConverter.to(definition()));

        if (tid != null)
            builder.setTaskId(tid);
        if (connected != null)
            builder.setConnectedTo(connected.toString());
        return builder.build();
    }

    protected abstract void onChunk(ByteString bytes) throws IOException;
}
