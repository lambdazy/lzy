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

import javax.annotation.Nullable;
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
    protected boolean persistent = false;
    protected String linkToStorage = null;

    LzyInputSlotBase(String tid, Slot definition) {
        super(definition);
        this.tid = tid;
    }

    @Override
    public void connectPersistent(URI slotUri) {
        persistent = true;
        connect(slotUri);
    }

    @Override
    public void connect(URI slotUri) {
        LOG.info("LzyInputSlotBase:: Attempt to connect to " + slotUri);
        if (servantSlotCh != null) {
            LOG.warn("Slot " + this + " was already connected");
            return;
        }

        connected = slotUri;
        servantSlotCh = ManagedChannelBuilder.forAddress(slotUri.getHost(), slotUri.getPort())
            .usePlaintext()
            .build();
        connectedSlotController = LzyServantGrpc.newBlockingStub(servantSlotCh);
    }

    @Override
    public @Nullable String getLinkToStorage() {
        return linkToStorage;
    }

    @Override
    public void disconnect() {
        LOG.info("LzyInputSlotBase:: disconnecting slot " + this);
        if (connected == null) {
            LOG.warn("Slot " + this + " was already disconnected");
            return;
        }
        servantSlotCh.shutdown();
        connected = null;
        servantSlotCh = null;
        LOG.info("LzyInputSlotBase:: disconnected " + this);
        state(Operations.SlotStatus.State.SUSPENDED);
    }

    protected void readAll() {
        final Iterator<Servant.Message> msgIter = connectedSlotController.openOutputSlot(Servant.SlotRequest.newBuilder()
            .setSlot(connected.getPath())
            .setOffset(offset)
            .setSlotUri(connected.toString())
            .build());
        try {
            ByteString result = ByteString.EMPTY;
            while (msgIter.hasNext()) {
                final Servant.Message next = msgIter.next();
                if (next.hasChunk()) {
                    final ByteString chunk = next.getChunk();
                    try {
                        LOG.info("From {} chunk received {}", name(), chunk.toString(StandardCharsets.UTF_8));
                        if (persistent) {
                            result = result.concat(chunk);
                        }
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
                    if (persistent) {
                        linkToStorage = saveToStorage(result);
                    }
                    break;
                }
            }
        }
        finally {
            LOG.info("Opening slot {}", name());
            state(Operations.SlotStatus.State.OPEN);
        }
    }

    private static String saveToStorage(ByteString str) {
        return "Link to a storage " + str.toStringUtf8();
    }

    @Override
    public Operations.SlotStatus status() {
        final Operations.SlotStatus.Builder builder = Operations.SlotStatus.newBuilder()
            .setState(state())
            .setPointer(offset)
            .setDeclaration(gRPCConverter.to(definition()));

        if (tid != null) {
            builder.setTaskId(tid);
        }
        if (connected != null) {
            builder.setConnectedTo(connected.toString());
        }
        return builder.build();
    }

    protected abstract void onChunk(ByteString bytes) throws IOException;
}
