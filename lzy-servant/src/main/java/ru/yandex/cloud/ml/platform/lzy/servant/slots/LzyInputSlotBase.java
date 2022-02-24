package ru.yandex.cloud.ml.platform.lzy.servant.slots;

import com.google.protobuf.ByteString;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.GrpcConverter;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzyInputSlot;
import ru.yandex.cloud.ml.platform.lzy.servant.slots.SlotConnectionManager.SlotController;
import ru.yandex.cloud.ml.platform.lzy.servant.snapshot.Snapshotter;
import yandex.cloud.priv.datasphere.v2.lzy.Operations;
import yandex.cloud.priv.datasphere.v2.lzy.Servant;

public abstract class LzyInputSlotBase extends LzySlotBase implements LzyInputSlot {
    private static final Logger LOG = LogManager.getLogger(LzyInputSlotBase.class);

    private final String tid;
    private long offset = 0;
    private URI connected;
    private SlotController slotController;

    LzyInputSlotBase(String tid, Slot definition, Snapshotter snapshotter) {
        super(definition, snapshotter);
        this.tid = tid;
    }

    @Override
    public void connect(URI slotUri, SlotController slotController) {
        connected = slotUri;
        this.slotController = slotController;
    }


    @Override
    public void disconnect() {
        LOG.info("LzyInputSlotBase:: disconnecting slot " + this);
        if (connected == null) {
            LOG.warn("Slot " + this + " was already disconnected");
            return;
        }
        connected = null;
        slotController = null;
        LOG.info("LzyInputSlotBase:: disconnected " + this);
        state(Operations.SlotStatus.State.SUSPENDED);
    }

    protected void readAll() {
        final Iterator<Servant.Message> msgIter = slotController.openOutputSlot(Servant.SlotRequest.newBuilder()
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
        } catch (StatusRuntimeException e) {
            LOG.error("InputSlotBase:: Failed openOutputSlot connection to servant " + connected, e);
        } finally {
            LOG.info("Opening slot {}", name());
            state(Operations.SlotStatus.State.OPEN);
        }
    }

    protected void readAllFromSnapshot() {
        String storageUrl = snapshotter.storageUrlForEntry(snapshotId, entryId);
        String storagePath = URI.create(storageUrl).getPath();
        String[] parts = storagePath.split("/");
        String bucket;
        String key;
        if (storagePath.startsWith("/")) {
            bucket = parts[1];
            key = Arrays.stream(parts).skip(2).collect(Collectors.joining("/"));
        } else {
            bucket = parts[0];
            key = Arrays.stream(parts).skip(1).collect(Collectors.joining("/"));
        }
        snapshotter.snapshotProvider().slotSnapshot(definition())
            .readFromStorage(bucket, key, data -> {
                LOG.info("From {} chunk received {}", name(), data.toString(StandardCharsets.UTF_8));
                onChunk(data);
            }, () -> state(Operations.SlotStatus.State.OPEN));
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

    protected abstract void onChunk(ByteString bytes) throws IOException;

    @Override
    public void snapshot(String snapshotId, String entryId) {
        super.snapshot(snapshotId, entryId);
        readAllFromSnapshot();
    }
}
