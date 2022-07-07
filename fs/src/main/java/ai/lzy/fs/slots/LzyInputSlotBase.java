package ai.lzy.fs.slots;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.net.URI;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ai.lzy.model.GrpcConverter;
import ai.lzy.model.Slot;
import ai.lzy.fs.fs.LzyInputSlot;
import ai.lzy.priv.v2.Operations;

import javax.annotation.Nullable;

public abstract class LzyInputSlotBase extends LzySlotBase implements LzyInputSlot {
    private static final Logger LOG = LogManager.getLogger(LzyInputSlotBase.class);

    private final String tid;
    private long offset = 0;
    private URI connected;
    private Stream<ByteString> dataProvider;

    protected LzyInputSlotBase(String tid, Slot definition) {
        super(definition);
        this.tid = tid;
    }

    @Override
    public void connect(URI slotUri, Stream<ByteString> dataProvider) {
        this.connected = slotUri;
        this.dataProvider = dataProvider;
    }

    @Override
    public void disconnect() {
        LOG.info("LzyInputSlotBase:: disconnecting slot " + this);
        if (connected == null) {
            LOG.warn("Slot " + this + " was already disconnected");
            return;
        }
        connected = null;
        LOG.info("LzyInputSlotBase:: disconnected {}", toString());
        state(Operations.SlotStatus.State.SUSPENDED);
    }

    protected void readAll() {
        try (final Stream<ByteString> data = dataProvider) {
            data.forEach(chunk -> {
                try {
                    LOG.debug("From {} received chunk of size {}", name(), chunk.size());
                    onChunk(chunk);
                } catch (IOException ioe) {
                    LOG.warn(
                        "Unable write chunk of data of size " + chunk.size() + " to input slot " + name(),
                        ioe
                    );
                } finally {
                    offset += chunk.size();
                }
            });
            dataProvider.close();
        } catch (Exception e) {
            LOG.error("InputSlotBase:: Failed openOutputSlot connection to servant " + connected, e);
            close();
            return;
        }
        LOG.info("Opening slot {}", name());
        state(Operations.SlotStatus.State.OPEN);
    }

    @Override
    public Operations.SlotStatus status() {
        final Operations.SlotStatus.Builder builder = Operations.SlotStatus.newBuilder()
            .setState(state())
            .setPointer(offset)
            .setDeclaration(GrpcConverter.to(definition()));

        if (tid != null) {
            builder.setTaskId(tid);
        }
        if (connected != null) {
            builder.setConnectedTo(connected.toString());
        }
        return builder.build();
    }

    @Nullable
    public URI connectedTo() {
        return connected;
    }
}
