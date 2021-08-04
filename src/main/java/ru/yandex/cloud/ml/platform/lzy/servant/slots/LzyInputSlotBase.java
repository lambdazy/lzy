package ru.yandex.cloud.ml.platform.lzy.servant.slots;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.gRPCConverter;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzyInputSlot;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServantGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.Operations;
import yandex.cloud.priv.datasphere.v2.lzy.Servant;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;

public abstract class LzyInputSlotBase extends LzySlotBase implements LzyInputSlot {
    private static final Logger LOG = Logger.getLogger(LzyInputSlotBase.class);

    private final String tid;
    private long offset = 0;
    private URI connected;
    private ManagedChannel servantSlotCh;

    public LzyInputSlotBase(String tid, Slot definition) {
        super(definition);
        this.tid = tid;
    }

    @Override
    public void connect(URI servant, String slot) {
        connected = servant;
        servantSlotCh = ManagedChannelBuilder.forAddress(
            servant.getHost(),
            servant.getPort()
        ).build();
        final LzyServantGrpc.LzyServantBlockingStub stub = LzyServantGrpc.newBlockingStub(servantSlotCh);

        final Iterator<Servant.Message> msgIter = stub.openOutputSlot(Servant.SlotRequest.newBuilder()
            .setSlot(slot)
            .setOffset(offset)
            .build());
        state(Operations.SlotStatus.State.OPEN);
        fjPool.execute(() -> {
            while(msgIter.hasNext()) {
                final Servant.Message next = msgIter.next();
                if (next.hasChunk()) {
                    final ByteString chunk = next.getChunk();
                    try {
                        onChunk(chunk);
                    }
                    catch (IOException ioe) {
                        LOG.warn("Unable write chunk of data of size " + chunk.size() + " to input slot " + name(), ioe);
                    }
                    finally {
                        offset += chunk.size();
                    }
                }
                else if (next.getControl() == Servant.Message.Controls.EOS) {
                    close();
                    return;
                }
            }
            disconnect();
        });
    }

    @Override
    public void disconnect() {
        servantSlotCh.shutdown();
        connected = null;
        servantSlotCh = null;
        state(Operations.SlotStatus.State.SUSPENDED);
    }

    @Override
    public void close() {
        disconnect();
        state(Operations.SlotStatus.State.CLOSED);
    }

    @Override
    public Operations.SlotStatus status() {
        final Operations.SlotStatus.Builder builder = Operations.SlotStatus.newBuilder()
            .setState(state())
            .setPointer(offset)
            .setDeclaration(gRPCConverter.to(definition()))
            .setTaskId(tid);
        if (connected != null)
            builder.setConnectedTo(connected.toString());
        return builder.build();
    }

    protected abstract void onChunk(ByteString bytes) throws IOException;
}
