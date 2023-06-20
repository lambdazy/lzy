package ai.lzy.fs.slots;

import ai.lzy.fs.fs.LzyInputSlot;
import ai.lzy.model.grpc.ProtoConverter;
import ai.lzy.model.slot.SlotInstance;
import ai.lzy.v1.common.LMS;
import com.google.protobuf.ByteString;
import jakarta.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.util.stream.Stream;

public abstract class LzyInputSlotBase extends LzySlotBase implements LzyInputSlot {
    private static final Logger LOG = LogManager.getLogger(LzyInputSlotBase.class);

    private long offset = 0;
    private URI connected;
    private Stream<ByteString> dataProvider;

    protected LzyInputSlotBase(SlotInstance instance) {
        super(instance);
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
        state(LMS.SlotStatus.State.SUSPENDED);
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
        } catch (Exception e) {
            LOG.error("InputSlotBase:: Failed openOutputSlot connection to worker " + connected, e);
            close();
            return;
        }
        onFinish();
        LOG.info("Opening slot {}", name());
        state(LMS.SlotStatus.State.OPEN);
    }

    protected void onFinish() {
        // intentionally blank
    }

    @Override
    public LMS.SlotStatus status() {
        final LMS.SlotStatus.Builder builder = LMS.SlotStatus.newBuilder()
            .setState(state())
            .setPointer(offset)
            .setDeclaration(ProtoConverter.toProto(definition()))
            .setTaskId(taskId());

        if (connected != null) {
            builder.setConnectedTo(connected.toString());
        }
        return builder.build();
    }

    @Nullable
    @Override
    public URI connectedTo() {
        return connected;
    }
}
