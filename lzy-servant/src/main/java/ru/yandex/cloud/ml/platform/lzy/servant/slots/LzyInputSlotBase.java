package ru.yandex.cloud.ml.platform.lzy.servant.slots;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.gRPCConverter;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzyInputSlot;
import yandex.cloud.priv.datasphere.v2.lzy.LzyKharonGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServantGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.Operations;
import yandex.cloud.priv.datasphere.v2.lzy.Servant;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

import static io.grpc.stub.ClientCalls.blockingServerStreamingCall;

public abstract class LzyInputSlotBase extends LzySlotBase implements LzyInputSlot {
    private static final Logger LOG = LogManager.getLogger(LzyInputSlotBase.class);

    private final String tid;
    private long offset = 0;
    private URI connected;
    private ManagedChannel servantSlotCh;

    interface SlotController {
        Iterator<Servant.Message> openOutputSlot(Servant.SlotRequest request);
    }
    private SlotController connectedSlotController;

    LzyInputSlotBase(String tid, Slot definition) {
        super(definition);
        this.tid = tid;
    }

    public void connect(URI slotUri, URI kharonUri) {
        LOG.info("LzyInputSlotBase:: Attempt to connect to " + slotUri);
        if (servantSlotCh != null) {
            LOG.warn("Slot " + this + " was already connected");
            return;
        }

        connected = slotUri;
        servantSlotCh = ManagedChannelBuilder.forAddress(kharonUri.getHost(), kharonUri.getPort())
                .usePlaintext()
                .build();
        connectedSlotController = new SlotController() {
            private final LzyKharonGrpc.LzyKharonBlockingStub stub = LzyKharonGrpc.newBlockingStub(servantSlotCh);
            @Override
            public Iterator<Servant.Message> openOutputSlot(Servant.SlotRequest request) {
                return stub.openOutputSlot(request);
            }
        };
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
        connectedSlotController = new SlotController() {
            private final LzyServantGrpc.LzyServantBlockingStub stub = LzyServantGrpc.newBlockingStub(servantSlotCh);
            @Override
            public Iterator<Servant.Message> openOutputSlot(Servant.SlotRequest request) {
                return stub.openOutputSlot(request);
            }
        };
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
            LOG.info("Opening slot {}", name());
            state(Operations.SlotStatus.State.OPEN);
        }
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
